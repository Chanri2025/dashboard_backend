# routers/consumption.py
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime

# ---- your helpers (import them from your utils module) ----
from utils.date_utils import (parse_start_timestamp, parse_end_timestamp)
from utils.mongo_helpers import convert_decimal128

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

router = APIRouter()


def _iso(dt: datetime) -> str:
    """Return ISO-like string in local IST (+0530) for consistency with earlier endpoints."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


@router.get("/consumption")
def get_consolidated_consumption(
        start_date: str = Query(..., description="YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]"),
        end_date: str = Query(..., description="YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]"),
        consumer_id: Optional[str] = Query(None, description="If given, filters by consumer_id"),
) -> List[Dict[str, Any]]:
    """
    Consolidated consumption from:
      1) powercasting.LT_Consumer_Consumption
         - Timestamp: ISO Date
         - Consumer_id, Dtr_id
         - Theoretical_kWh, Energy_consumption_kWh
      2) powercasting.open_aceess_consumer_consumption
         - timestamp: 'DD/MM/YYYY HH:MM' (string, IST; may contain bad/NaN values)

    Returns a single list with unified fields and time-sorted.
    """
    # ---- Parse input windows ----
    try:
        start = parse_start_timestamp(start_date)
        end = parse_end_timestamp(end_date)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    if not MONGO_URI:
        raise HTTPException(status_code=500, detail="MONGO_URI is not configured")

    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client["powercasting"]

        # ===================== 1) LT collection =====================
        lt_query: Dict[str, Any] = {"Timestamp": {"$gte": start, "$lte": end}}
        if consumer_id:
            lt_query["Consumer_id"] = consumer_id

        lt_coll = db["LT_Consumer_Consumption"]
        lt_docs = lt_coll.find(
            lt_query,
            {
                "_id": False,
                "Timestamp": True,
                "Consumer_id": True,
                "Dtr_id": True,
                "Theoretical_kWh": True,
                "Energy_consumption_kWh": True,
            },
        )

        lt_out: List[Dict[str, Any]] = []
        for d in lt_docs:
            d = convert_decimal128(d)  # handles Decimal128 if present
            ts = d.get("Timestamp")
            if isinstance(ts, datetime):
                iso_ts = _iso(ts)
            else:
                # fallback: try parse various string-ish forms
                try:
                    iso_ts = _iso(datetime.fromisoformat(str(ts)))
                except Exception:
                    iso_ts = str(ts)

            lt_out.append(
                {
                    "source": "LT",
                    "consumer_id": d.get("Consumer_id"),
                    "timestamp": iso_ts,
                    "consumption_kwh": d.get("Energy_consumption_kWh", 0.0),
                    "injection_kwh": None,
                    "theoretical_kwh": d.get("Theoretical_kWh"),
                    "dtr_id": d.get("Dtr_id"),
                }
            )

        # ===================== 2) OA collection (ultra-safe) =====================
        oa_pipeline: List[Dict[str, Any]] = []

        # Optional early filter by consumer_id
        if consumer_id:
            oa_pipeline.append({"$match": {"consumer_id": consumer_id}})

        oa_pipeline += [
            # Detect type and array-ness once
            {
                "$addFields": {
                    "_ts_type": {"$type": "$timestamp"},
                    "_ts_is_array": {"$isArray": "$timestamp"},
                }
            },

            # Keep a candidate string ONLY if timestamp is a scalar string
            {
                "$addFields": {
                    "ts_str": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$_ts_is_array", False]},
                                    {"$eq": ["$_ts_type", "string"]},
                                ]
                            },
                            {"$trim": {"input": "$timestamp"}},
                            None,
                        ]
                    }
                }
            },

            # Drop empty or literal 'NaN'/'nan'/'null'/'NULL' strings → set to None
            {
                "$addFields": {
                    "ts_str": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": [{"$type": "$ts_str"}, "string"]},
                                    {
                                        "$or": [
                                            {"$eq": ["$ts_str", ""]},
                                            {
                                                "$regexMatch": {
                                                    "input": "$ts_str",
                                                    "regex": r"^(nan|null|NaN|NULL)$"
                                                }
                                            },
                                        ]
                                    },
                                ]
                            },
                            None,
                            "$ts_str",
                        ]
                    }
                }
            },

            # Parse ONLY when ts_str is a string (branch-protected)
            {
                "$addFields": {
                    "ts": {
                        "$cond": [
                            {"$eq": [{"$type": "$ts_str"}, "string"]},
                            {
                                "$dateFromString": {
                                    "dateString": "$ts_str",
                                    "format": "%d/%m/%Y %H:%M",
                                    "timezone": "Asia/Kolkata",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            None,
                        ]
                    }
                }
            },

            # Fallback parse (lenient) ONLY if first parse failed but ts_str is still a string
            {
                "$addFields": {
                    "ts": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$eq": ["$ts", None]},
                                    {"$eq": [{"$type": "$ts_str"}, "string"]},
                                ]
                            },
                            {
                                "$dateFromString": {
                                    "dateString": "$ts_str",
                                    "timezone": "Asia/Kolkata",
                                    "onError": None,
                                    "onNull": None,
                                }
                            },
                            "$ts",
                        ]
                    }
                }
            },

            # Keep only docs with a successfully parsed datetime
            {"$match": {"ts": {"$ne": None}}},

            # Apply the window
            {"$match": {"ts": {"$gte": start, "$lte": end}}},

            # Final projection and sort
            {
                "$project": {
                    "_id": 0,
                    "consumer_id": 1,
                    "consumption": 1,
                    "injection": 1,
                    "ts": 1,
                }
            },
            {"$sort": {"ts": 1}},
        ]

        oa_coll = db["open_aceess_consumer_consumption"]
        oa_docs = list(oa_coll.aggregate(oa_pipeline))

        oa_out: List[Dict[str, Any]] = []
        for d in oa_docs:
            d = convert_decimal128(d)
            ts = d.get("ts")
            iso_ts = _iso(ts) if isinstance(ts, datetime) else str(ts)
            oa_out.append({
                "source": "OA",
                "consumer_id": d.get("consumer_id"),
                "timestamp": iso_ts,
                "consumption_kwh": d.get("consumption", 0.0),
                "injection_kwh": d.get("injection"),
                "theoretical_kwh": None,
                "dtr_id": None,
            })

        # ===================== Merge & sort =====================
        merged = lt_out + oa_out
        # Sort by parsed timestamps (strings are already ISO-like)
        merged.sort(key=lambda r: r.get("timestamp", ""))

        return merged

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mongo error: {e}")
    finally:
        if client:
            client.close()

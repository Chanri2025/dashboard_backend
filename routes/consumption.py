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
    # return ISO string in local IST (+0530) for consistency with earlier endpoints
    # if you prefer UTC .isoformat(), adjust accordingly
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
         - timestamp: 'DD/MM/YYYY HH:MM' (string, IST)
         - consumer_id, consumption, injection

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
        lt_docs = lt_coll.find(lt_query, {
            "_id": False,
            "Timestamp": True,
            "Consumer_id": True,
            "Dtr_id": True,
            "Theoretical_kWh": True,
            "Energy_consumption_kWh": True,
        })

        lt_out: List[Dict[str, Any]] = []
        for d in lt_docs:
            d = convert_decimal128(d)  # handles Decimal128 if present
            ts = d.get("Timestamp")
            if isinstance(ts, datetime):
                iso_ts = _iso(ts)
            else:
                # Just in case, try to parse string-ish timestamps
                try:
                    iso_ts = _iso(datetime.fromisoformat(str(ts)))
                except Exception:
                    iso_ts = str(ts)

            lt_out.append({
                "source": "LT",
                "consumer_id": d.get("Consumer_id"),
                "timestamp": iso_ts,
                "consumption_kwh": d.get("Energy_consumption_kWh", 0.0),
                "injection_kwh": None,
                "theoretical_kwh": d.get("Theoretical_kWh"),
                "dtr_id": d.get("Dtr_id"),
                # raw fields if you need them later:
                # "_raw": d
            })

        # ===================== 2) OA collection =====================
        # OA stores 'timestamp' as string "DD/MM/YYYY HH:MM" in IST; convert with $dateFromString
        oa_pipeline: List[Dict[str, Any]] = [
            {
                "$addFields": {
                    "ts": {
                        "$dateFromString": {
                            "dateString": "$timestamp",
                            "format": "%d/%m/%Y %H:%M",
                            "timezone": "Asia/Kolkata"
                        }
                    }
                }
            },
            {
                "$match": {
                    "ts": {"$gte": start, "$lte": end}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "consumer_id": 1,
                    "consumption": 1,
                    "injection": 1,
                    "ts": 1
                }
            },
            {"$sort": {"ts": 1}}
        ]
        if consumer_id:
            # exact match on consumer_id (OA)
            oa_pipeline.insert(1, {"$match": {"consumer_id": consumer_id}})

        oa_coll = db["open_aceess_consumer_consumption"]
        oa_docs = list(oa_coll.aggregate(oa_pipeline))

        oa_out: List[Dict[str, Any]] = []
        for d in oa_docs:
            d = convert_decimal128(d)
            ts = d.get("ts")
            if isinstance(ts, datetime):
                iso_ts = _iso(ts)
            else:
                iso_ts = str(ts)

            oa_out.append({
                "source": "OA",
                "consumer_id": d.get("consumer_id"),
                "timestamp": iso_ts,
                "consumption_kwh": d.get("consumption", 0.0),
                "injection_kwh": d.get("injection"),
                "theoretical_kwh": None,
                "dtr_id": None,
                # "_raw": d
            })

        # ===================== Merge & sort =====================
        merged = lt_out + oa_out
        merged.sort(key=lambda r: r.get("timestamp", ""))

        return merged

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mongo error: {e}")
    finally:
        if client:
            client.close()

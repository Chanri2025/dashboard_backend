# routers/consumption.py
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import math
import os
import logging

from fastapi import APIRouter, HTTPException, Query, Request
from pymongo import MongoClient
from dotenv import load_dotenv

# ---- helpers ----
from utils.date_utils import parse_start_timestamp, parse_end_timestamp
from utils.mongo_helpers import convert_decimal128

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

logger = logging.getLogger("consumer")
logger.setLevel(logging.INFO)

router = APIRouter()


def _iso_utc(dt: datetime) -> str:
    """Return ISO-8601 string in UTC, e.g. '2023-04-01T00:15:00+00:00'."""
    if not isinstance(dt, datetime):
        return str(dt)
    return dt.astimezone(timezone.utc).isoformat()


def _num(x: Any, default: float = 0.0) -> float:
    """
    Sanitize numeric values so JSON never contains NaN/Inf.
    Decimal128/str/int/float -> float; None/NaN/Inf/-Inf -> default.
    """
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


@router.get("/consumption")
def get_consumer_consumption(
        request: Request,
        start_date: str = Query(..., description="YYYY-MM-DD or 'YYYY-MM-DD HH:MM[:SS]'"),
        end_date: str = Query(..., description="YYYY-MM-DD or 'YYYY-MM-DD HH:MM[:SS]'"),
        consumer_id: Optional[str] = Query(None, description="Optional; filters by Consumer_id"),
) -> List[Dict[str, Any]]:
    """
    SAFE endpoint that ONLY queries the consolidated 'Consumer_consumption' collection via .find().
    It does not touch raw OA/LT collections (so no $dateFromString / NaN aggregation issues).

    Returns objects with:
      - Timestamp (UTC ISO string)
      - Consumer_id
      - Dtr_id (can be null for OA rows)
      - Theoretical_kWh (can be null for OA rows; returned as 0.0 if null)
      - Energy_consumption_kWh
      - Injection_kWh (unified key; supports Injection_kWh or Injection_KWh in DB)
    """
    # Parse window
    try:
        start = parse_start_timestamp(start_date)
        end = parse_end_timestamp(end_date)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    if not MONGO_URI:
        raise HTTPException(status_code=500, detail="MONGO_URI is not configured")

    client: Optional[MongoClient] = None
    try:
        client = MongoClient(MONGO_URI)
        db = client["powercasting"]
        coll = db["Consumer_consumption"]

        # Query
        query: Dict[str, Any] = {"Timestamp": {"$gte": start, "$lte": end}}
        if consumer_id:
            query["Consumer_id"] = consumer_id

        # Project both injection casings; keep only what we need
        projection = {
            "_id": False,
            "Timestamp": True,
            "Consumer_id": True,
            "Dtr_id": True,
            "Theoretical_kWh": True,
            "Energy_consumption_kWh": True,
            "Injection_kWh": True,  # lower-k (as in your samples)
            "Injection_KWh": True,  # upper-K/W (if present in your DB)
        }

        cursor = coll.find(query, projection).sort("Timestamp", 1)

        out: List[Dict[str, Any]] = []
        for doc in cursor:
            # Normalize Decimal128 → python types
            doc = convert_decimal128(doc)

            # Timestamp
            ts = doc.get("Timestamp")
            ts_str = _iso_utc(ts) if isinstance(ts, datetime) else str(ts)

            # Unified injection field name (prefer lower-k; fallback to upper-KW)
            inj_raw = doc.get("Injection_kWh", None)
            if inj_raw is None:
                inj_raw = doc.get("Injection_KWh", 0.0)
            injection = _num(inj_raw, default=0.0)

            # Numerics (OA rows can have Theoretical_kWh=None; we return 0.0)
            theoretical = _num(doc.get("Theoretical_kWh"), default=0.0)
            energy = _num(doc.get("Energy_consumption_kWh"), default=0.0)

            out.append(
                {
                    "Timestamp": ts_str,
                    "Consumer_id": doc.get("Consumer_id"),
                    "Dtr_id": doc.get("Dtr_id"),  # can be None for OA rows
                    "Theoretical_kWh": theoretical,
                    "Energy_consumption_kWh": energy,
                    "Injection_kWh": injection,
                }
            )

        return out

    except HTTPException:
        raise
    except Exception as e:
        # If you ever see $dateFromString here, you are hitting a DIFFERENT route that aggregates OA.
        raise HTTPException(status_code=500, detail=f"Mongo error: {e}")
    finally:
        if client:
            client.close()

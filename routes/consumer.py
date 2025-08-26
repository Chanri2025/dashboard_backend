# routers/consumption.py
from typing import Optional, List, Dict, Any
from datetime import datetime
import os

from fastapi import APIRouter, HTTPException, Query, Depends, status
from pymongo import MongoClient
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import select, or_, func

# --- helpers ---
from utils.date_utils import parse_start_timestamp, parse_end_timestamp
from utils.mongo_helpers import convert_decimal128

# --- SQLAlchemy (MySQL) ---
from Connections.db_sql_consumers import get_db
from Models.consumer_model import ConsumerDetails
from Schemas.consumer_schema import (
    ConsumerDetailsCreate,
    ConsumerDetailsUpdate,
    ConsumerDetailsOut,
)

# ───────────────────────── ENV ─────────────────────────
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# ───────────────────────── ROUTERS ─────────────────────────
# Keep them separate to avoid any path conflicts.
router = APIRouter()


# =========================================================================================
#                                CONSUMPTION (Mongo, consolidated)
# =========================================================================================
def _iso(dt: datetime) -> str:
    """Return ISO-like string (no timezone suffix)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


@router.get("/consumption")
def get_consolidated_consumption(
        start_date: str = Query(..., description="YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]"),
        end_date: str = Query(..., description="YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]"),
        consumer_id: Optional[str] = Query(None, description="If given, filters by consumer_id"),
) -> List[Dict[str, Any]]:
    """
    Consolidated consumption from MongoDB:

    1) powercasting.LT_Consumer_Consumption
       - Timestamp: ISO Date
       - Consumer_id, Dtr_id
       - Theoretical_kWh, Energy_consumption_kWh

    2) powercasting.open_aceess_consumer_consumption
       - timestamp: 'DD/MM/YYYY HH:MM' (string, IST)
       - consumer_id, consumption, injection

    Returns a single list with unified fields and time-sorted.
    """
    # Validate input window
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

        # ---------- 1) LT collection ----------
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
            d = convert_decimal128(d)
            ts = d.get("Timestamp")
            if isinstance(ts, datetime):
                iso_ts = _iso(ts)
            else:
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

        # ---------- 2) OA collection ----------
        # OA 'timestamp' is "DD/MM/YYYY HH:MM" (string, IST) → convert server-side using $dateFromString
        oa_pipeline: List[Dict[str, Any]] = [
            {
                "$addFields": {
                    "ts": {
                        "$dateFromString": {
                            "dateString": "$timestamp",
                            "format": "%d/%m/%Y %H:%M",
                            "timezone": "Asia/Kolkata",
                        }
                    }
                }
            },
            {"$match": {"ts": {"$gte": start, "$lte": end}}},
            {"$project": {"_id": 0, "consumer_id": 1, "consumption": 1, "injection": 1, "ts": 1}},
            {"$sort": {"ts": 1}},
        ]
        if consumer_id:
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

            oa_out.append(
                {
                    "source": "OA",
                    "consumer_id": d.get("consumer_id"),
                    "timestamp": iso_ts,
                    "consumption_kwh": d.get("consumption", 0.0),
                    "injection_kwh": d.get("injection"),
                    "theoretical_kwh": None,
                    "dtr_id": None,
                }
            )

        # Merge & sort
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


# =========================================================================================
#                              CONSUMER DETAILS CRUD (MySQL)
# =========================================================================================

@router.post("/", response_model=ConsumerDetailsOut, status_code=status.HTTP_201_CREATED)
def create_consumer(payload: ConsumerDetailsCreate, db: Session = Depends(get_db)):
    exists = db.execute(
        select(ConsumerDetails).where(func.upper(ConsumerDetails.consumer_id) == func.upper(payload.consumer_id))
    ).scalar_one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="consumer_id already exists")

    obj = ConsumerDetails(**payload.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.get("/", response_model=List[ConsumerDetailsOut])
def list_consumers(
        db: Session = Depends(get_db),
        q: Optional[str] = None,
        skip: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=200),
):
    stmt = select(ConsumerDetails)
    if q:
        like = f"%{q}%"
        stmt = stmt.where(
            or_(
                ConsumerDetails.consumer_id.ilike(like),
                ConsumerDetails.circle.ilike(like),
                ConsumerDetails.division.ilike(like),
                ConsumerDetails.Name.ilike(like),
                ConsumerDetails.District.ilike(like),
            )
        )
    rows = db.execute(stmt.offset(skip).limit(limit)).scalars().all()
    return rows


@router.get("/{id:int}", response_model=ConsumerDetailsOut)
def get_consumer(id: int, db: Session = Depends(get_db)):
    obj = db.get(ConsumerDetails, id)
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    return obj


@router.get("/by-code/{consumer_id}", response_model=ConsumerDetailsOut)
def get_consumer_by_code(consumer_id: str, db: Session = Depends(get_db)):
    obj = db.execute(
        select(ConsumerDetails).where(func.upper(ConsumerDetails.consumer_id) == func.upper(consumer_id))
    ).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    return obj


@router.put("/{id:int}", response_model=ConsumerDetailsOut)
def update_consumer(id: int, payload: ConsumerDetailsUpdate, db: Session = Depends(get_db)):
    obj = db.get(ConsumerDetails, id)
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")

    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(obj, k, v)

    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.delete("/{id:int}", status_code=status.HTTP_204_NO_CONTENT)
def delete_consumer(id: int, db: Session = Depends(get_db)):
    obj = db.get(ConsumerDetails, id)
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(obj)
    db.commit()
    return None

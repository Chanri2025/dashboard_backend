# routers/consumption.py
from typing import Optional, List, Dict, Any
from datetime import datetime
import os
import logging

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

# ───────────────────────── LOGGING / SANITIZATION ─────────────────────────
logger = logging.getLogger(__name__)

# hard limits from your Pydantic schema
VOLTS_MIN, VOLTS_MAX = 1, 1000  # conint(gt=0, le=1000)
SANCTION_MIN = 1  # conint(gt=0)
OA_MIN = 0  # conint(ge=0)

REQUIRED_FALLBACK = "UNKNOWN"  # fallback for required strings if DB row is dirty


def _row_to_dict(row) -> Dict[str, Any]:
    """Convert a SQLAlchemy model instance to a plain dict (excluding SQLA internals)."""
    return {k: v for k, v in row.__dict__.items() if not k.startswith("_")}


def _as_int(v: Any) -> Optional[int]:
    """Best-effort parse to int; returns None on failure."""
    try:
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                return None
            # accept "66", "66.0"
            return int(float(v))
    except Exception:
        return None
    return None


def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def _sanitize_required_str(s: Any, max_len: int) -> str:
    """Ensure a non-empty string for required text fields; fallback to 'UNKNOWN'."""
    if s is None:
        return REQUIRED_FALLBACK
    s = str(s).strip()
    if not s:
        return REQUIRED_FALLBACK
    return s[:max_len]


def _sanitize_consumer_payload(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Conform DB row to Pydantic schema constraints so response_model validation passes.
    - voltage_kv: int, gt 0, le 1000
    - sanction_load_kw: int, gt 0
    - oa_capacity_kw: int, ge 0
    - required strings: ensure non-empty
    """
    changed = False

    # Integers with bounds
    v_int = _as_int(d.get("voltage_kv"))
    if v_int is None or v_int <= 0:
        v_new = VOLTS_MIN
    else:
        v_new = _clamp(v_int, VOLTS_MIN, VOLTS_MAX)
    if v_new != d.get("voltage_kv"):
        d["voltage_kv"] = v_new
        changed = True

    s_int = _as_int(d.get("sanction_load_kw"))
    if s_int is None or s_int <= 0:
        s_new = SANCTION_MIN
    else:
        s_new = max(SANCTION_MIN, s_int)
    if s_new != d.get("sanction_load_kw"):
        d["sanction_load_kw"] = s_new
        changed = True

    oa_int = _as_int(d.get("oa_capacity_kw"))
    if oa_int is None or oa_int < 0:
        oa_new = OA_MIN
    else:
        oa_new = max(OA_MIN, oa_int)
    if oa_new != d.get("oa_capacity_kw"):
        d["oa_capacity_kw"] = oa_new
        changed = True

    # Required strings in schema (min_length=1): consumer_id, circle, division, consumer_type
    # Also trim to their respective max lengths defined in your schema
    # consumer_id: max 50
    cid = d.get("consumer_id")
    cid_new = _sanitize_required_str(cid, 50)
    if cid_new != cid:
        d["consumer_id"] = cid_new
        changed = True

    # circle: max 100
    circle = d.get("circle")
    circle_new = _sanitize_required_str(circle, 100)
    if circle_new != circle:
        d["circle"] = circle_new
        changed = True

    # division: max 150
    division = d.get("division")
    division_new = _sanitize_required_str(division, 150)
    if division_new != division:
        d["division"] = division_new
        changed = True

    # consumer_type: max 50
    ctype = d.get("consumer_type")
    ctype_new = _sanitize_required_str(ctype, 50)
    if ctype_new != ctype:
        d["consumer_type"] = ctype_new
        changed = True

    if changed:
        ident = d.get("id") or d.get("consumer_id") or "<unknown>"
        logger.info(f"[consumer_sanitize] Coerced row {ident} to satisfy response schema")

    return d


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
                    "theoretical_kwh": d.get("Theoretical_KWh") if "Theoretical_KWh" in d else d.get("Theoretical_kWh"),
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

    d = _row_to_dict(obj)
    d = _sanitize_consumer_payload(d)
    return d


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

    sanitized: List[Dict[str, Any]] = []
    for r in rows:
        d = _row_to_dict(r)
        d = _sanitize_consumer_payload(d)
        sanitized.append(d)
    return sanitized


@router.get("/{id:int}", response_model=ConsumerDetailsOut)
def get_consumer(id: int, db: Session = Depends(get_db)):
    obj = db.get(ConsumerDetails, id)
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    d = _row_to_dict(obj)
    d = _sanitize_consumer_payload(d)
    return d


@router.get("/by-code/{consumer_id}", response_model=ConsumerDetailsOut)
def get_consumer_by_code(consumer_id: str, db: Session = Depends(get_db)):
    obj = db.execute(
        select(ConsumerDetails).where(func.upper(ConsumerDetails.consumer_id) == func.upper(consumer_id))
    ).scalar_one_or_none()
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    d = _row_to_dict(obj)
    d = _sanitize_consumer_payload(d)
    return d


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
    d = _row_to_dict(obj)
    d = _sanitize_consumer_payload(d)
    return d


@router.delete("/{id:int}", status_code=status.HTTP_204_NO_CONTENT)
def delete_consumer(id: int, db: Session = Depends(get_db)):
    obj = db.get(ConsumerDetails, id)
    if not obj:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(obj)
    db.commit()
    return None

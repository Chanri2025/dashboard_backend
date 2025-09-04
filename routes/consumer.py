from __future__ import annotations

from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import os
import logging
from pydantic import BaseModel

from fastapi import APIRouter, HTTPException, Query, Depends, status, Body, Path
from pymongo import MongoClient
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import select, or_, func

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

# ───────────────────────── ROUTER ─────────────────────────
router = APIRouter()

# ───────────────────────── LOGGING / SANITIZATION ─────────────────────────
logger = logging.getLogger(__name__)

VOLTS_MIN, VOLTS_MAX = 1, 1000
SANCTION_MIN = 1
OA_MIN = 0

REQUIRED_FALLBACK = "UNKNOWN"


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
            s = v.strip()
            if s == "":
                return None
            return int(float(s))
    except Exception:
        return None
    return None


def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def _sanitize_required_str(s: Any, max_len: int) -> str:
    """Ensure a non-empty string for required text fields; fallback to 'UNKNOWN'."""
    if s is None:
        return REQUIRED_FALLBACK
    s = str(s).replace("\r", "").replace("\n", "").strip()
    if not s:
        return REQUIRED_FALLBACK
    return s[:max_len]


def _strip_crlf(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).replace("\r", "").replace("\n", "").strip()
    return s


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
    v_new = VOLTS_MIN if (v_int is None or v_int <= 0) else _clamp(v_int, VOLTS_MIN, VOLTS_MAX)
    if v_new != d.get("voltage_kv"):
        d["voltage_kv"] = v_new
        changed = True

    s_int = _as_int(d.get("sanction_load_kw"))
    s_new = SANCTION_MIN if (s_int is None or s_int <= 0) else max(SANCTION_MIN, s_int)
    if s_new != d.get("sanction_load_kw"):
        d["sanction_load_kw"] = s_new
        changed = True

    oa_int = _as_int(d.get("oa_capacity_kw"))
    oa_new = OA_MIN if (oa_int is None or oa_int < 0) else max(OA_MIN, oa_int)
    if oa_new != d.get("oa_capacity_kw"):
        d["oa_capacity_kw"] = oa_new
        changed = True

    # Required strings in schema: consumer_id, circle, division, consumer_type
    cid = d.get("consumer_id")
    cid_new = _sanitize_required_str(cid, 50)
    if cid_new != cid:
        d["consumer_id"] = cid_new
        changed = True

    circle = d.get("circle")
    circle_new = _sanitize_required_str(circle, 100)
    if circle_new != circle:
        d["circle"] = circle_new
        changed = True

    division = d.get("division")
    division_new = _sanitize_required_str(division, 150)
    if division_new != division:
        d["division"] = division_new
        changed = True

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


# =========================================================================================
#                           CONSUMER DETAILS CRUD (MySQL)
# =========================================================================================
class ConsumerCreateIn(BaseModel):
    """
    Flexible create input from UI.  NO feeder_id anymore.
    We accept division directly and keep DTR_id as optional free-text.
    """
    # core
    consumer_id: Optional[str] = None
    circle: Optional[str] = None
    division: Optional[str] = None  # stored as-is

    voltage_kv: Optional[Any] = None  # accept str/number -> int
    sanction_load_kw: Optional[Any] = None
    oa_capacity_kw: Optional[Any] = None
    consumer_type: Optional[str] = None

    # optional details
    Name: Optional[str] = None
    Address: Optional[str] = None
    District: Optional[str] = None
    PinCode: Optional[str] = None
    DTR_id: Optional[str] = None


@router.post("/", response_model=ConsumerDetailsOut, status_code=status.HTTP_201_CREATED)
def create_consumer(
        payload_in: ConsumerCreateIn = Body(...),
        db: Session = Depends(get_db),
):
    """
    Create consumer:
    - Accepts division directly (no feeder aliasing)
    - Coerces number fields from strings
    - Fills missing required text with 'UNKNOWN' for DB-required columns
    """
    if not payload_in.consumer_id or not str(payload_in.consumer_id).strip():
        raise HTTPException(status_code=422, detail="consumer_id is required")

    # 1) Resolve/normalize required strings
    consumer_id = _sanitize_required_str(payload_in.consumer_id, 50)
    circle = _sanitize_required_str(payload_in.circle, 100)
    division = _sanitize_required_str(payload_in.division, 150)  # ensure non-empty for DB write
    consumer_type = _sanitize_required_str(payload_in.consumer_type, 50)

    # 2) Numbers with coercion + bounds
    v_int = _as_int(payload_in.voltage_kv)
    s_int = _as_int(payload_in.sanction_load_kw)
    oa_int = _as_int(payload_in.oa_capacity_kw)

    voltage_kv = VOLTS_MIN if (v_int is None or v_int <= 0) else _clamp(v_int, VOLTS_MIN, VOLTS_MAX)
    sanction_load_kw = SANCTION_MIN if (s_int is None or s_int <= 0) else max(SANCTION_MIN, s_int)
    oa_capacity_kw = OA_MIN if (oa_int is None or oa_int < 0) else max(OA_MIN, oa_int)

    # 3) Enforce uniqueness by consumer_id (case-insensitive)
    exists = db.execute(
        select(ConsumerDetails).where(func.upper(ConsumerDetails.consumer_id) == func.upper(consumer_id))
    ).scalar_one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="consumer_id already exists")

    # 4) Build strict schema for DB write (no feeder_id anywhere)
    strict_payload = ConsumerDetailsCreate(
        consumer_id=consumer_id,
        circle=circle,
        division=division,
        voltage_kv=voltage_kv,
        sanction_load_kw=sanction_load_kw,
        oa_capacity_kw=oa_capacity_kw,
        consumer_type=consumer_type,
        Name=payload_in.Name,
        Address=payload_in.Address,
        District=payload_in.District,
        PinCode=payload_in.PinCode,
        DTR_id=payload_in.DTR_id,
    )

    # exclude None fields before constructing ORM model
    data = strict_payload.model_dump(exclude_none=True)

    obj = ConsumerDetails(**data)
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

    data = payload.model_dump(exclude_unset=True)

    # Coerce/clamp numeric fields if provided
    if "voltage_kv" in data and data["voltage_kv"] is not None:
        v = _as_int(data["voltage_kv"])
        data["voltage_kv"] = VOLTS_MIN if (v is None or v <= 0) else _clamp(v, VOLTS_MIN, VOLTS_MAX)
    if "sanction_load_kw" in data and data["sanction_load_kw"] is not None:
        s = _as_int(data["sanction_load_kw"])
        data["sanction_load_kw"] = SANCTION_MIN if (s is None or s <= 0) else max(SANCTION_MIN, s)
    if "oa_capacity_kw" in data and data["oa_capacity_kw"] is not None:
        oa = _as_int(data["oa_capacity_kw"])
        data["oa_capacity_kw"] = OA_MIN if (oa is None or oa < 0) else max(OA_MIN, oa)

    # Sanitize required strings if present
    for key, max_len in [("consumer_id", 50), ("circle", 100), ("division", 150), ("consumer_type", 50)]:
        if key in data and data[key] is not None:
            data[key] = _sanitize_required_str(data[key], max_len)

    for k, v in data.items():
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


@router.get("/by-dtr/{dtr_id}", response_model=List[ConsumerDetailsOut])
def get_consumers_by_dtr(dtr_id: str, db: Session = Depends(get_db)):
    """
    Fetch all consumers for a given DTR (dtr_id).
    - Case-insensitive
    - Ignores trailing/leading CR/LF in dirty DB rows (e.g. 'FEEDER1_DTR1\\r')
    """
    norm_input = _strip_crlf(dtr_id)
    if not norm_input:
        raise HTTPException(status_code=400, detail="dtr_id is required")

    col_norm = func.upper(
        func.replace(
            func.replace(func.trim(ConsumerDetails.DTR_id), '\r', ''),
            '\n', ''
        )
    )

    rows = db.execute(
        select(ConsumerDetails).where(col_norm == func.upper(norm_input))
    ).scalars().all()

    if not rows:
        rows = db.execute(
            select(ConsumerDetails).where(col_norm.like(func.upper(norm_input) + "%"))
        ).scalars().all()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No consumers found for dtr_id={norm_input}")

    return [_sanitize_consumer_payload(_row_to_dict(r)) for r in rows]

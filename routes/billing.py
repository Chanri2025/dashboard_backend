# routers/billing.py
from typing import List, Optional, Dict, Any, Tuple
from decimal import Decimal
from datetime import datetime
import os

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.decimal128 import Decimal128

from Connections.db_sql_consumers import get_db
from Models.billing_models import TariffPlan, TariffSlab, ConsumerTariff, Bill, BillLine
from Schemas.billing_schema import (
    TariffPlanCreate, TariffPlanOut, TariffSlabCreate, AssignTariff,
    BillPreviewRequest, BillOut, BillLineOut, BillStatusUpdate
)
from utils.date_utils import parse_start_timestamp, parse_end_timestamp
from Models.consumer_model import ConsumerDetails

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

router = APIRouter()


# ───────────────────────── Helpers ─────────────────────────

def _D(x) -> Decimal:
    return Decimal(str(x or 0))


def _to_float(x) -> float:
    """Safely convert Mongo Decimal128 / Decimal / numbers to float."""
    if isinstance(x, Decimal128):
        return float(x.to_decimal())
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(x or 0)
    except Exception:
        return 0.0


def _sum_lt_kwh(db_mongo, consumer_id: str, start: datetime, end: datetime) -> float:
    """Sum Energy_consumption_kWh from LT_Consumer_Consumption within window."""
    coll = db_mongo["LT_Consumer_Consumption"]
    pipeline = [
        {"$match": {"Consumer_id": consumer_id, "Timestamp": {"$gte": start, "$lte": end}}},
        # If your values are Decimal128, this project ensures doubles come out of $sum
        {"$project": {"Energy_consumption_kWh": {"$toDouble": "$Energy_consumption_kWh"}}},
        {"$group": {"_id": None, "kwh": {"$sum": "$Energy_consumption_kWh"}}}
    ]
    res = list(coll.aggregate(pipeline))
    if not res:
        return 0.0
    return _to_float(res[0].get("kwh"))


def _sum_oa_kwh(db_mongo, consumer_id: str, start: datetime, end: datetime) -> Tuple[float, float]:
    """
    Sum OA import/export from open_aceess_consumer_consumption.
    'timestamp' is "DD/MM/YYYY HH:MM" string (IST).
    Returns (import_kwh, export_kwh_abs).
    """
    coll = db_mongo["open_aceess_consumer_consumption"]
    pipeline = [
        {"$addFields": {
            "ts": {"$dateFromString": {
                "dateString": "$timestamp", "format": "%d/%m/%Y %H:%M", "timezone": "Asia/Kolkata"
            }},
            "consumption_d": {"$toDouble": "$consumption"},
            "injection_d": {"$toDouble": "$injection"},
        }},
        {"$match": {"consumer_id": consumer_id, "ts": {"$gte": start, "$lte": end}}},
        {"$group": {"_id": None, "import_kwh": {"$sum": "$consumption_d"}, "injection_sum": {"$sum": "$injection_d"}}}
    ]
    res = list(coll.aggregate(pipeline))
    if not res:
        return 0.0, 0.0
    imp = _to_float(res[0].get("import_kwh"))
    inj_sum = _to_float(res[0].get("injection_sum"))
    return imp, abs(inj_sum)


def _find_applicable_tariff(db: Session, consumer_id: str, start: datetime, end: datetime) -> Tuple[
    TariffPlan, List[TariffSlab]]:
    """Pick a tariff assignment overlapping the billing window; return plan + slabs."""
    ct = db.execute(
        select(ConsumerTariff)
        .where(
            (ConsumerTariff.consumer_id == consumer_id) &
            (ConsumerTariff.valid_from <= end.date()) &
            (func.coalesce(ConsumerTariff.valid_to, end.date()) >= start.date())
        )
        .order_by(ConsumerTariff.valid_from.desc())
    ).scalars().first()

    if not ct:
        raise HTTPException(status_code=404, detail="No tariff assigned for consumer in this period")

    plan = db.get(TariffPlan, ct.tariff_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Tariff plan not found")

    slabs = db.execute(
        select(TariffSlab).where(TariffSlab.tariff_id == plan.id)
        .order_by(TariffSlab.slab_from_kwh.asc())
    ).scalars().all()

    return plan, slabs


def _apply_slabs(kwh: float, slabs: List[TariffSlab]) -> Tuple[Decimal, List[BillLineOut]]:
    """Apply slab rates to kWh; return (energy_total, energy_lines)."""
    remaining = Decimal(str(kwh))
    total = Decimal("0")
    lines: List[BillLineOut] = []

    for s in slabs:
        start = Decimal(str(s.slab_from_kwh or 0))
        end = Decimal(str(s.slab_to_kwh)) if s.slab_to_kwh is not None else None
        rate = Decimal(str(s.energy_rate_per_kwh))

        if remaining <= 0:
            break

        # quantity within this slab
        if end is None:
            # Open-ended slab: everything above 'start'
            qty = max(Decimal("0"), remaining - max(Decimal("0"), start))
        else:
            # Bounded slab: from 'start' up to 'end'
            if remaining <= start:
                qty = Decimal("0")
            else:
                upper = min(remaining, end)
                qty = max(Decimal("0"), upper - start)

        if qty > 0:
            amount = qty * rate
            total += amount
            lines.append(BillLineOut(
                line_type="ENERGY",
                description=f"Slab {start} - {('∞' if end is None else end)} @ {rate}",
                quantity=float(qty),
                rate=float(rate),
                amount=float(amount.quantize(Decimal('0.0001')))
            ))

        # reduce remaining; if end is None, we consumed all
        if end is None:
            remaining = Decimal("0")
        else:
            # any kWh above 'end' remain for next slabs
            if remaining > end:
                remaining = remaining  # next loop handles further slabs
            else:
                remaining = Decimal("0")

    return total, lines


def _preview_bill(
        db: Session,
        consumer_id: str,
        meter_type: str,
        start: datetime,
        end: datetime,
        adjustments: Decimal
) -> BillOut:
    """Compute bill components without saving."""
    if meter_type not in ("LT", "OA"):
        raise HTTPException(status_code=400, detail="meter_type must be 'LT' or 'OA'")
    if not MONGO_URI:
        raise HTTPException(status_code=500, detail="MONGO_URI not configured")

    client = MongoClient(MONGO_URI)
    try:
        mongo = client["powercasting"]

        if meter_type == "LT":
            imp_kwh = _sum_lt_kwh(mongo, consumer_id, start, end)
            exp_kwh = 0.0
        else:
            imp_kwh, exp_kwh = _sum_oa_kwh(mongo, consumer_id, start, end)
    finally:
        client.close()

    plan, slabs = _find_applicable_tariff(db, consumer_id, start, end)

    energy_total, energy_lines = _apply_slabs(imp_kwh, slabs)
    fixed_charge = _D(plan.fixed_charge)
    tax_amount = (energy_total + fixed_charge) * (_D(plan.tax_percent) / Decimal("100"))
    total = energy_total + fixed_charge + tax_amount + adjustments

    # add fixed / adjustment / tax lines for transparency
    energy_lines.append(BillLineOut(
        line_type="FIXED", description="Fixed charge",
        quantity=1.0, rate=float(fixed_charge), amount=float(fixed_charge)
    ))
    if adjustments != 0:
        energy_lines.append(BillLineOut(
            line_type="ADJUSTMENT", description="Manual adjustments",
            quantity=1.0, rate=float(adjustments), amount=float(adjustments)
        ))
    if tax_amount != 0:
        energy_lines.append(BillLineOut(
            line_type="TAX", description=f"Tax {plan.tax_percent}%",
            quantity=1.0, rate=float(tax_amount), amount=float(tax_amount)
        ))

    return BillOut(
        id=None,
        consumer_id=consumer_id,
        meter_type=meter_type,
        period_start=start,
        period_end=end,
        kwh_import=float(Decimal(str(imp_kwh)).quantize(Decimal("0.000001"))),
        kwh_export=float(Decimal(str(exp_kwh)).quantize(Decimal("0.000001"))),
        fixed_charge=float(fixed_charge),
        energy_charge=float(energy_total.quantize(Decimal("0.0001"))),
        tax_amount=float(tax_amount.quantize(Decimal("0.0001"))),
        adjustments=float(adjustments),
        total_amount=float(total.quantize(Decimal("0.0001"))),
        status="DRAFT",
        lines=energy_lines
    )


# ───────────────────────── Endpoints ─────────────────────────

@router.post("/tariff", response_model=TariffPlanOut, status_code=status.HTTP_201_CREATED)
def create_tariff(plan: TariffPlanCreate, db: Session = Depends(get_db)):
    exists = db.execute(select(TariffPlan).where(TariffPlan.code == plan.code)).scalar_one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="Tariff code already exists")
    obj = TariffPlan(**plan.model_dump())
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


@router.post("/tariff/{tariff_id}/slabs", status_code=status.HTTP_201_CREATED)
def add_slabs(tariff_id: int, slabs: List[TariffSlabCreate], db: Session = Depends(get_db)):
    plan = db.get(TariffPlan, tariff_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Tariff not found")
    for s in slabs:
        db.add(TariffSlab(tariff_id=tariff_id, **s.model_dump()))
    db.commit()
    return {"ok": True, "added": len(slabs)}


@router.post("/assign-tariff", status_code=status.HTTP_201_CREATED)
def assign_tariff(payload: AssignTariff, db: Session = Depends(get_db)):
    # Optionally validate consumer_id exists in consumer_details before assigning
    db.add(ConsumerTariff(**payload.model_dump()))
    db.commit()
    return {"ok": True}


@router.post("/preview", response_model=BillOut)
def preview_bill(body: BillPreviewRequest, db: Session = Depends(get_db)):
    if body.period_start >= body.period_end:
        raise HTTPException(status_code=400, detail="period_start must be < period_end")
    adjustments = Decimal(str(body.adjustments or 0))
    return _preview_bill(db, body.consumer_id, body.meter_type, body.period_start, body.period_end, adjustments)


@router.post("/finalize", response_model=BillOut, status_code=status.HTTP_201_CREATED)
def finalize_bill(body: BillPreviewRequest, db: Session = Depends(get_db)):
    preview = preview_bill(body, db)

    # persist bill + lines
    bill = Bill(
        consumer_id=preview.consumer_id,
        meter_type=preview.meter_type,
        period_start=preview.period_start,
        period_end=preview.period_end,
        kwh_import=preview.kwh_import,
        kwh_export=preview.kwh_export,
        fixed_charge=preview.fixed_charge,
        energy_charge=preview.energy_charge,
        tax_amount=preview.tax_amount,
        adjustments=preview.adjustments,
        total_amount=preview.total_amount,
        status="ISSUED",
    )
    db.add(bill)
    db.flush()

    for ln in preview.lines:
        db.add(BillLine(
            bill_id=bill.id,
            line_type=ln.line_type,
            description=ln.description,
            quantity=ln.quantity,
            rate=ln.rate,
            amount=ln.amount
        ))
    db.commit()
    db.refresh(bill)

    # return the saved bill as BillOut
    preview.id = bill.id
    preview.status = "ISSUED"
    return preview


@router.get("/", response_model=List[BillOut])
def list_bills(
        db: Session = Depends(get_db),
        consumer_id: Optional[str] = None,
        meter_type: Optional[str] = None,
        status_q: Optional[str] = Query(None, alias="status"),
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        skip: int = 0, limit: int = 50
):
    stmt = select(Bill)
    if consumer_id:
        stmt = stmt.where(Bill.consumer_id == consumer_id)
    if meter_type:
        stmt = stmt.where(Bill.meter_type == meter_type)
    if status_q:
        stmt = stmt.where(Bill.status == status_q)
    if start_date:
        s = parse_start_timestamp(start_date)
        stmt = stmt.where(Bill.period_start >= s)
    if end_date:
        e = parse_end_timestamp(end_date)
        stmt = stmt.where(Bill.period_end <= e)
    stmt = stmt.order_by(Bill.period_start.desc()).offset(skip).limit(limit)

    rows = db.execute(stmt).scalars().all()
    out: List[BillOut] = []
    for b in rows:
        lines = db.execute(select(BillLine).where(BillLine.bill_id == b.id)).scalars().all()
        out.append(BillOut(
            id=b.id, consumer_id=b.consumer_id, meter_type=b.meter_type,
            period_start=b.period_start, period_end=b.period_end,
            kwh_import=float(b.kwh_import or 0), kwh_export=float(b.kwh_export or 0),
            fixed_charge=float(b.fixed_charge or 0), energy_charge=float(b.energy_charge or 0),
            tax_amount=float(b.tax_amount or 0), adjustments=float(b.adjustments or 0),
            total_amount=float(b.total_amount or 0), status=b.status,
            lines=[BillLineOut(
                line_type=ln.line_type, description=ln.description,
                quantity=float(ln.quantity or 0), rate=float(ln.rate or 0), amount=float(ln.amount or 0)
            ) for ln in lines]
        ))
    return out


@router.get("/{bill_id:int}", response_model=BillOut)
def get_bill(bill_id: int, db: Session = Depends(get_db)):
    b = db.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Bill not found")
    lines = db.execute(select(BillLine).where(BillLine.bill_id == b.id)).scalars().all()
    return BillOut(
        id=b.id, consumer_id=b.consumer_id, meter_type=b.meter_type,
        period_start=b.period_start, period_end=b.period_end,
        kwh_import=float(b.kwh_import or 0), kwh_export=float(b.kwh_export or 0),
        fixed_charge=float(b.fixed_charge or 0), energy_charge=float(b.energy_charge or 0),
        tax_amount=float(b.tax_amount or 0), adjustments=float(b.adjustments or 0),
        total_amount=float(b.total_amount or 0), status=b.status,
        lines=[BillLineOut(
            line_type=ln.line_type, description=ln.description,
            quantity=float(ln.quantity or 0), rate=float(ln.rate or 0), amount=float(ln.amount or 0)
        ) for ln in lines]
    )


@router.put("/{bill_id:int}/status", response_model=BillOut)
def update_bill_status(bill_id: int, body: BillStatusUpdate, db: Session = Depends(get_db)):
    b = db.get(Bill, bill_id)
    if not b:
        raise HTTPException(status_code=404, detail="Bill not found")
    if body.status not in ("DRAFT", "ISSUED", "PAID", "CANCELLED"):
        raise HTTPException(status_code=400, detail="Invalid status")
    b.status = body.status
    db.add(b)
    db.commit()
    db.refresh(b)
    return get_bill(bill_id, db)


# ───────────────────────── Tariff APIs ─────────────────────────

@router.get("/tariff", response_model=List[dict])
def list_tariffs(db: Session = Depends(get_db)):
    """Return tariffs with their slabs included"""
    tariffs = db.execute(select(TariffPlan)).scalars().all()
    out = []
    for t in tariffs:
        slabs = db.execute(
            select(TariffSlab).where(TariffSlab.tariff_id == t.id)
        ).scalars().all()
        out.append({
            "id": t.id,
            "code": t.code,
            "consumer_type": t.consumer_type,
            "voltage_kv": t.voltage_kv,
            "fixed_charge": t.fixed_charge,
            "tax_percent": t.tax_percent,
            "demand_charge_per_kw": t.demand_charge_per_kw,
            "effective_from": t.effective_from,
            "slabs": [
                {
                    "id": s.id,
                    "slab_from_kwh": s.slab_from_kwh,
                    "slab_to_kwh": s.slab_to_kwh,
                    "energy_rate_per_kwh": s.energy_rate_per_kwh
                }
                for s in slabs
            ]
        })
    return out


@router.get("/tariff/{tariff_id}", response_model=TariffPlanOut)
def get_tariff(tariff_id: int, db: Session = Depends(get_db)):
    """Get single tariff plan by ID"""
    plan = db.get(TariffPlan, tariff_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Tariff not found")
    return plan


@router.get("/tariff/{tariff_id}/slabs", response_model=List[TariffSlabCreate])
def get_tariff_slabs(tariff_id: int, db: Session = Depends(get_db)):
    """Get all slabs for a given tariff"""
    slabs = db.execute(
        select(TariffSlab).where(TariffSlab.tariff_id == tariff_id).order_by(TariffSlab.slab_from_kwh.asc())
    ).scalars().all()
    return slabs


# ───────────────────── Consumer Tariff Assignment ─────────────────────

@router.get("/consumer/{consumer_id}/tariffs")
def list_consumer_tariffs(consumer_id: str, db: Session = Depends(get_db)):
    """
    List all tariff assignments of a consumer.
    Shows history if multiple tariffs assigned across periods.
    """
    rows = db.execute(
        select(ConsumerTariff)
        .where(ConsumerTariff.consumer_id == consumer_id)
        .order_by(ConsumerTariff.valid_from.desc())
    ).scalars().all()

    out = []
    for ct in rows:
        plan = db.get(TariffPlan, ct.tariff_id)
        out.append({
            "assignment_id": ct.id,
            "consumer_id": ct.consumer_id,
            "tariff_id": ct.tariff_id,
            "tariff_code": plan.code if plan else None,
            "valid_from": ct.valid_from,
            "valid_to": ct.valid_to,
            "created_at": ct.created_at,
        })
    return out


@router.delete("/consumer/tariff/{assignment_id}")
def delete_consumer_tariff(assignment_id: int, db: Session = Depends(get_db)):
    """Remove a consumer tariff assignment"""
    ct = db.get(ConsumerTariff, assignment_id)
    if not ct:
        raise HTTPException(status_code=404, detail="Assignment not found")
    db.delete(ct)
    db.commit()
    return {"ok": True, "deleted": assignment_id}


@router.get("/eligible-consumers")
def list_eligible_consumers(db: Session = Depends(get_db)):
    """
    Return only consumers who have at least one tariff assigned.
    Uses ConsumerDetails table.
    """
    consumers = db.execute(select(ConsumerDetails)).scalars().all()
    eligible = []

    for c in consumers:
        tariffs = db.execute(
            select(ConsumerTariff).where(ConsumerTariff.consumer_id == c.consumer_id)
        ).scalars().all()
        if tariffs:
            eligible.append({
                "consumer_id": c.consumer_id,
                "circle": c.circle,
                "division": c.division,
                "voltage_kv": c.voltage_kv,
                "sanction_load_kw": c.sanction_load_kw,
                "oa_capacity_kw": c.oa_capacity_kw,
                "consumer_type": c.consumer_type,
                "Name": c.Name,
                "Address": c.Address,
                "District": c.District,
                "PinCode": c.PinCode,
                "DTR_id": c.DTR_id,
            })

    return eligible


@router.put("/tariff/{tariff_id}", response_model=TariffPlanOut)
def update_tariff(tariff_id: int, plan: TariffPlanCreate, db: Session = Depends(get_db)):
    """Update an existing tariff plan"""
    obj = db.get(TariffPlan, tariff_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Tariff not found")

    # prevent duplicate codes if updating code
    exists = db.execute(
        select(TariffPlan).where(TariffPlan.code == plan.code, TariffPlan.id != tariff_id)
    ).scalar_one_or_none()
    if exists:
        raise HTTPException(status_code=409, detail="Tariff code already exists")

    for field, value in plan.model_dump().items():
        setattr(obj, field, value)

    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

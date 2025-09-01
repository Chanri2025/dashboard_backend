# routes/power_theft.py
from fastapi import APIRouter, Query
from typing import Optional, Dict, Any, List, Tuple
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from datetime import datetime
import os

# your utils
from utils.date_utils import parse_iso_timestamp, parse_start_timestamp, parse_end_timestamp

router = APIRouter()

def get_db():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://DMMPrice:Babai6157201@147.93.106.173:27017/"))
    return client["powercasting"]

def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal128):
        return float(v.to_decimal())
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None

def parse_mongo_timestamp(v) -> Optional[datetime]:
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        dt = parse_iso_timestamp(v)
        if dt:
            return dt
    return None

def floor_to_15min(dt: datetime) -> datetime:
    minute_bucket = (dt.minute // 15) * 15
    return dt.replace(minute=minute_bucket, second=0, microsecond=0)

def classify_primary_event(loss_pct: Optional[float]) -> Tuple[str, float]:
    # severity as 0.0–1.0 (code2 mapping)
    if loss_pct is None:
        return "Invalid Data", 0.0
    if loss_pct > 50:
        return "Magnetic tamper", 1.0
    elif 30 < loss_pct <= 50:
        return "Meter top cover open", 0.8
    elif 15 < loss_pct <= 30:
        return "Current/Voltage Missing or Current unbalance", 0.6
    elif 5 < loss_pct <= 15:
        return "Current/Voltage unbalance or Voltage unbalance", 0.4
    elif 0 <= loss_pct <= 5:
        return "Normal", 0.1
    else:
        return "Reverse Feed / Error", 0.2

def map_risk(severity: Optional[float]) -> str:
    if severity is None or severity <= 0.2:
        return "No Risk / Normal"
    elif severity <= 0.5:
        return "Low Risk"
    elif severity <= 0.75:
        return "Medium Risk"
    elif severity <= 1.0:
        return "High Risk"
    return "Critical"

# kWh per 15-min bounds — same as your dashboard, converted to kWh
APPLIANCE_UNITS: Dict[str, Tuple[float, float]] = {
    "1 Ton AC, 3-Star": (180/1000, 220/1000),
    "1.5 Ton AC, 3-Star": (270/1000, 360/1000),
    "2 Ton AC, 3-Star": (360/1000, 450/1000),
    "1.5 Ton AC, 5-Star (Inverter)": (220/1000, 270/1000),
    "2 Ton AC, 5-Star (Inverter)": (320/1000, 400/1000),
    "Fridge 200L, 3-Star": (30/1000, 35/1000),
    "Fridge 300L, 5-Star": (25/1000, 30/1000),
    "Fridge 500L, Side-by-Side": (40/1000, 60/1000),
    "Washing Machine - Top Load": (10/1000, 13/1000),
    "Washing Machine - Front Load": (12/1000, 15/1000),
    "Washing Machine - Front Load (Inverter)": (6/1000, 10/1000),
    "LED TV 32\"": (7/1000, 10/1000),
    "LED TV 42\" Smart": (12/1000, 15/1000),
    "OLED/QLED 55\"": (18/1000, 25/1000),
    "Laptop - Basic": (8/1000, 10/1000),
    "Laptop - Gaming": (15/1000, 20/1000),
    "Desktop PC - Standard": (20/1000, 30/1000),
    "Desktop PC - Gaming/Workstation": (40/1000, 60/1000),
    "Microwave Oven - Solo 20L": (12/1000, 15/1000),
    "Microwave Oven - Convection 25L": (18/1000, 25/1000),
    "Electric Kettle - Standard 1.5L": (5/1000, 8/1000),
    "Geyser - Instant (10L)": (20/1000, 25/1000),
    "Geyser - Storage (25L)": (30/1000, 40/1000),
    "Geyser - Solar + Electric Hybrid": (15/1000, 20/1000),
    "Induction Cooktop - Single Zone": (45/1000, 60/1000),
    "Induction Cooktop - Double Zone": (60/1000, 90/1000),
    "Ceiling Fan - Standard": (14/1000, 18/1000),
    "Ceiling Fan - BEE 5-Star (BLDC)": (6/1000, 9/1000),
    "Light Tube 20W": (3/1000, 4/1000),
    "Light Bulb 9W": (1.5/1000, 2/1000),
    "Mobile Charger Fast": (0/1000, 1/1000),
    "Mobile Charger Standard": (0/1000, 1/1000),
    "Water Pump 0.5 HP": (10/1000, 12/1000),
    "Water Pump 1 HP": (22/1000, 25/1000),
    "Air Cooler Personal": (35/1000, 50/1000),
    "Air Cooler Desert": (45/1000, 60/1000),
    "Mixer/Grinder": (3/1000, 5/1000),
    "Toaster": (2/1000, 5/1000),
    "Room Heater": (60/1000, 120/1000),
    "Hair Dryer": (2/1000, 5/1000),
    "Iron": (10/1000, 18/1000),
}

def detect_appliance(kwh_15min: float) -> str:
    for name, (low, high) in APPLIANCE_UNITS.items():
        if low <= kwh_15min <= high:
            return name
    return "Other"

def appliance_overuse_metrics(kwh_15min: float, app: str, margin: float) -> Tuple[bool, float, float, float]:
    """
    Returns: (is_overuse, high_bound, threshold, over_ratio)
    over_ratio = actual / threshold  (>=1 means overuse if is_overuse True)
    """
    if app == "Other":
        return (False, 0.0, 0.0, 0.0)
    low, high = APPLIANCE_UNITS.get(app, (0.0, 0.0))
    threshold = high * (1.0 + margin) if high > 0 else 0.0
    if high > 0 and kwh_15min > threshold:
        return (True, high, threshold, (kwh_15min / threshold) if threshold > 0 else 0.0)
    return (False, high, threshold, (kwh_15min / threshold) if threshold > 0 else 0.0)

# Diagnosis policy:
# - Theft severity from Loss_Percent → Base_Severity (0–1) → theft_score=Base_Severity*100
# - Appliance overuse if Actual_kWh > high_bound*(1+margin):
#   overuse_score = 20 + 60 * clamp(over_ratio - 1, 0..1)  (+10 if ≥4 consecutive blocks), cap 100
# - Final severity = max(theft_score, overuse_score)

def priority_from_score(score: float) -> str:
    if score >= 90: return "Critical"
    if score >= 70: return "High"
    if score >= 40: return "Medium"
    if score >= 20: return "Low"
    return "Normal"

def action_recommendation(scenario: str, app: str, loss_pct: Optional[float]) -> str:
    if scenario == "Theft Suspected - Tamper":
        return "Dispatch field team; check meter sealing, wiring, and CT/PT integrity; capture site photos."
    if scenario == "Theft Suspected - Anomaly":
        return "Validate meter health and wiring; schedule inspection; compare with historical baselines."
    if scenario == "Appliance Overuse":
        base = f"Advise reducing run-time/setting for {app}. "
        tip = "Use timers/eco mode; check thermostat/insulation/filters."
        return base + tip
    if scenario == "Normal":
        return "No action needed."
    return "Review interval data and confirm on-site usage patterns."

@router.get("/", summary="Diagnosis per 15-min block: appliance overuse vs theft, with severity & actions")
def diagnose_blocks(
    customer_id: Optional[str] = Query(default=None),
    start: Optional[str] = Query(default=None, description="YYYY-MM-DD or ISO"),
    end: Optional[str] = Query(default=None, description="YYYY-MM-DD or ISO"),
    limit: int = Query(default=500, ge=1, le=20000),
    overuse_margin: float = Query(default=0.15, ge=0.0, le=1.0)
):
    db = get_db()
    coll = db["Consumer_consumption"]

    # Build query
    query: Dict[str, Any] = {}
    if customer_id:
        query["Consumer_id"] = customer_id

    start_dt = parse_iso_timestamp(start) if start else None
    end_dt = parse_iso_timestamp(end) if end else None
    if not start_dt and start:
        try: start_dt = parse_start_timestamp(start)
        except Exception: pass
    if not end_dt and end:
        try: end_dt = parse_end_timestamp(end)
        except Exception: pass

    if start_dt or end_dt:
        ts_filter: Dict[str, Any] = {}
        if start_dt: ts_filter["$gte"] = start_dt
        if end_dt:   ts_filter["$lte"] = end_dt
        query["Timestamp"] = ts_filter

    cursor = coll.find(
        query,
        projection={"_id": 0, "Timestamp": 1, "Energy_consumption_kWh": 1, "Theoretical_kWh": 1, "Consumer_id": 1},
    )

    # Aggregate into 15-min buckets (chronological for streaks)
    buckets: Dict[datetime, Dict[str, Any]] = {}
    for doc in cursor:
        ts = parse_mongo_timestamp(doc.get("Timestamp"))
        if not ts:
            continue
        period = floor_to_15min(ts)
        actual = to_float(doc.get("Energy_consumption_kWh")) or 0.0
        theoretical = to_float(doc.get("Theoretical_kWh")) or 0.0
        b = buckets.get(period)
        if b is None:
            b = {"Period_15min": period, "Actual_kWh": 0.0, "Theoretical_kWh": 0.0}
            buckets[period] = b
        b["Actual_kWh"] += actual
        b["Theoretical_kWh"] += theoretical

    if not buckets:
        return {"records": [], "count": 0, "filters": {
            "customer_id": customer_id,
            "start": start_dt.isoformat() if start_dt else None,
            "end": end_dt.isoformat() if end_dt else None,
            "limit": limit,
            "overuse_margin": overuse_margin
        }}

    # Process buckets oldest → newest to track overuse streaks
    periods_sorted = sorted(buckets.keys())
    overuse_streaks: Dict[str, int] = {}

    enriched: List[Dict[str, Any]] = []
    for period in periods_sorted:
        b = buckets[period]
        actual = b["Actual_kWh"]
        theoretical = b["Theoretical_kWh"]
        energy_loss = theoretical - actual
        loss_pct = (energy_loss / theoretical) * 100.0 if theoretical > 0 else None

        # Theft
        primary_event, base_sev = classify_primary_event(loss_pct)
        risk_level = map_risk(base_sev)
        theft_score = base_sev * 100.0

        # Appliance
        app = detect_appliance(actual)
        is_over, high_bound, threshold, over_ratio = appliance_overuse_metrics(actual, app, overuse_margin)

        # Overuse score
        overuse_score = 0.0
        if is_over:
            overuse_score = 20.0 + 60.0 * max(0.0, min(1.0, (over_ratio - 1.0)))
            streak = overuse_streaks.get(app, 0) + 1
            overuse_streaks[app] = streak
            if streak >= 4:
                overuse_score = min(100.0, overuse_score + 10.0)
        else:
            overuse_streaks[app] = 0

        # Final severity & scenario
        final_score = max(theft_score, overuse_score)
        if theft_score >= overuse_score and base_sev >= 0.6:
            scenario = "Theft Suspected - Anomaly" if base_sev < 1.0 else "Theft Suspected - Tamper"
        elif is_over:
            scenario = "Appliance Overuse"
        else:
            scenario = "Normal" if base_sev <= 0.2 else "Efficiency Issue"

        action = action_recommendation(scenario, app, loss_pct)
        rationale = []
        if is_over:
            rationale.append(f"{app} over threshold ({actual:.3f} kWh > {threshold:.3f} kWh)")
            if overuse_streaks.get(app, 0) >= 4:
                rationale.append("repeated overuse observed")
        if base_sev >= 0.6 and loss_pct is not None:
            rationale.append(f"loss {loss_pct:.1f}% → {primary_event}")
        if not rationale:
            rationale.append("within expected range")

        enriched.append({
            "Period_15min": period.isoformat(),
            "Actual_kWh": round(actual, 6),
            "Theoretical_kWh": round(theoretical, 6),
            "Energy_Loss_kWh": round(energy_loss, 6),
            "Loss_Percent": (round(loss_pct, 6) if loss_pct is not None else None),
            "Primary_Event": primary_event,
            "Base_Severity": base_sev,
            "Risk_Level": risk_level,
            "Likely_Appliance": app,
            "Overuse": is_over,
            "Overuse_Threshold_kWh": round(threshold, 6),
            "Overuse_Ratio": round(over_ratio, 6),
            "Scenario": scenario,
            "Severity_Score": round(final_score, 1),
            "Priority": priority_from_score(final_score),
            "Recommended_Action": action,
            "Rationale": "; ".join(rationale)
        })

    # newest first + limit
    enriched.sort(key=lambda r: r["Period_15min"], reverse=True)
    enriched = enriched[:limit]

    return {
        "records": enriched,
        "count": len(enriched),
        "filters": {
            "customer_id": customer_id,
            "start": start_dt.isoformat() if start_dt else None,
            "end": end_dt.isoformat() if end_dt else None,
            "limit": limit,
            "overuse_margin": overuse_margin
        }
    }

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from datetime import time
from dotenv import load_dotenv
import os
import math
from Helpers.helpers import parse_start_timestamp

router = APIRouter()
load_dotenv()

# === DB Connections ===
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(mongo_uri)
power_db = client["power_casting_new"]


# === Helpers ===

def fetch_adjusted_units(ts):
    rec = power_db["banking_data"].find_one({"Timestamp": ts}, {"_id": 0, "adjusted_units": 1})
    if not rec:
        raise LookupError(f"No banking_data for {ts}")
    au = rec.get("adjusted_units", 0.0) or 0.0
    return 0.0 if math.isnan(au) else au


def fetch_battery_status(ts):
    prev = power_db["Battery_Status"].find_one({"Timestamp": ts}, sort=[("Timestamp", -1)])
    if not prev:
        prev = power_db["Battery_Status"].find_one({"Timestamp": {"$lt": ts}}, sort=[("Timestamp", -1)])
    if not prev:
        raise LookupError(f"No Battery_Status before or at {ts}")
    return prev


def fetch_plant_data(ts):
    cursor = power_db["Plant_Generation"].find(
        {"Timestamp": ts},
        {"_id": 0, "Plant_Name": 1, "DC": 1, "SG": 1, "VC": 1}
    )
    plants = list(cursor)

    for p in plants:
        dc = 0.0 if math.isnan(p.get("DC", 0.0) or 0.0) else p["DC"]
        sg = 0.0 if math.isnan(p.get("SG", 0.0) or 0.0) else p["SG"]
        vc = round(0.0 if math.isnan(p.get("VC", 0.0) or 0.0) else p["VC"], 2)

        bd = round(((dc - sg) * 1000 * 0.25) if dc > sg else 0.0, 2)
        cost = round(bd * vc if not math.isnan(bd * vc) else 0.0, 2)

        p.update({
            "DC": dc,
            "SG": sg,
            "VC": vc,
            "backdown_units": bd,
            "backdown_cost": cost
        })

    plants.sort(key=lambda p: p["VC"], reverse=True)
    return plants


def fetch_market_prices(ts):
    rec = power_db["market_price_data"].find_one(
        {"Timestamp": ts},
        {"_id": 0, "DAM": 1, "RTM": 1, "Market_Purchase": 1}
    )
    if not rec:
        raise LookupError(f"No market_price_data for {ts}")
    dam = rec.get("DAM", 0.0) or 0.0
    rtm = rec.get("RTM", 0.0) or 0.0
    market_purchase = rec.get("Market_Purchase", 0.0) or 0.0
    return dam, rtm, market_purchase


def upsert_battery_status(ts, banked_units, cycle):
    prev = fetch_battery_status(ts)
    prev_units = prev.get("Units_Available", 0.0) or 0.0

    if cycle == "CHARGE":
        new_units = prev_units - banked_units if prev_units > banked_units else prev_units - (banked_units - prev_units)
    elif cycle == "USE":
        new_units = prev_units
    else:
        new_units = prev_units

    new_units = round(new_units, 3)
    power_db["Battery_Status"].update_one(
        {"Timestamp": ts},
        {"$set": {"Units_Available": new_units, "Cycle": cycle}},
        upsert=True
    )


def in_dsm_window(ts):
    t = ts.time()
    return (time(9, 0) <= t < time(11, 0)) or (time(18, 0) <= t < time(20, 0))


# === FastAPI Endpoint ===

@router.get("/calculate")
def calculate_adjustment(start_date: str = Query(...)):
    # Step 1: Parse timestamp
    try:
        ts = parse_start_timestamp(start_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Step 2: Fetch adjusted units
    try:
        adjustment_unit = fetch_adjusted_units(ts)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if adjustment_unit <= 0:
        return {
            "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),
            "adjustment_unit": adjustment_unit,
            "adjustment_charges": 0.0,
        }

    # Step 3: Battery info
    status_doc = fetch_battery_status(ts)
    cycle = status_doc.get("Cycle", "").upper()
    available_units = status_doc.get("Units_Available", 0.0)

    # Step 4: Backdown analysis
    plants = fetch_plant_data(ts)
    total_backdown_units = sum(p["backdown_units"] for p in plants)
    total_backdown_cost = sum(p["backdown_cost"] for p in plants)
    mod_price = round(plants[0]['VC'], 2) if total_backdown_units > 0 else 0.0

    # Step 5: Market prices
    dam, rtm, _ = fetch_market_prices(ts)
    highest_rate = max(mod_price, dam, rtm)
    BATTERY_CHARGE_RATE = 4.0

    # Step 6: Adjustment Logic
    if (cycle == "USE") and in_dsm_window(ts):
        adjustment_charges = round(adjustment_unit * highest_rate, 2)
        balance_unit = 0.0
        battery_used = adjustment_unit
        upsert_battery_status(ts, adjustment_unit, "CHARGE")
    else:
        if adjustment_unit < available_units:
            battery_used = 0.0
            balance_unit = 0.0
            upsert_battery_status(ts, adjustment_unit, "NO CHARGE")
            adjustment_charges = round(battery_used * BATTERY_CHARGE_RATE, 2)
        else:
            battery_used = 0.0
            balance_unit = adjustment_unit - available_units
            upsert_battery_status(ts, balance_unit, "NO CHARGE")
            adjustment_charges = round(
                battery_used * BATTERY_CHARGE_RATE + balance_unit * highest_rate,
                2
            )

    return JSONResponse(content={
        "Backdown_units": total_backdown_units,
        "Backdown_cost": total_backdown_cost,
        "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),
        "adjustment_unit": adjustment_unit,
        "battery_cycle": cycle,
        "battery_units_available": available_units,
        "battery_units_charge": battery_used,
        "balance_units": balance_unit,
        "weighted_avg_rate": mod_price,
        "dam_rate": dam,
        "rtm_rate": rtm,
        "highest_rate": highest_rate,
        "battery_charge_rate": BATTERY_CHARGE_RATE,
        "adjustment_charges": adjustment_charges
    })

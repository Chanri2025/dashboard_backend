# routes/banking.py
from fastapi import APIRouter, HTTPException, Query
from pymongo import MongoClient
from datetime import time, datetime
from typing import Optional
import math
import os

from dotenv import load_dotenv
from Helpers.helpers import parse_start_timestamp

router = APIRouter()
load_dotenv()

# MongoDB connection
mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
power_db = client["power_casting_new"]


def fetch_banked_units(ts):
    rec = power_db["banking_data"].find_one({"Timestamp": ts}, {"_id": 0, "banked_units": 1})
    if not rec:
        raise LookupError(f"No banking_data for {ts}")
    return 0.0 if math.isnan(rec.get("banked_units", 0.0) or 0.0) else rec.get("banked_units", 0.0)


def fetch_plant_data(ts):
    cursor = power_db["Plant_Generation"].find(
        {"Timestamp": ts}, {"_id": 0, "Plant_Name": 1, "DC": 1, "SG": 1, "VC": 1}
    )
    plants = list(cursor)
    for p in plants:
        dc, sg, vc = p.get("DC", 0.0), p.get("SG", 0.0), p.get("VC", 0.0)
        p["DC"] = 0.0 if math.isnan(dc) else dc
        p["SG"] = 0.0 if math.isnan(sg) else sg
        p["VC"] = round(0.0 if math.isnan(vc) else vc, 2)
        bd = round(((p["DC"] - p["SG"]) * 1000 * 0.25) if p["DC"] > p["SG"] else 0.0, 2)
        p["backdown_units"] = bd
        cost = round(bd * p["VC"] if not math.isnan(bd * p["VC"]) else 0.0, 2)
        p["backdown_cost"] = cost
    return plants


def fetch_demand_drawl(ts):
    rec = power_db["Demand_Drawl"].find_one({"Timestamp": ts}, {"_id": 0, "Scheduled_Generation": 1, "Drawl": 1})
    if not rec:
        raise LookupError(f"No Demand_Drawl for {ts}")
    return rec.get("Scheduled_Generation", 0.0), rec.get("Drawl", 0.0)


def fetch_market_prices(ts):
    rec = power_db["market_price_data"].find_one(
        {"Timestamp": ts}, {"_id": 0, "DAM": 1, "RTM": 1, "Market_Purchase": 1}
    )
    if not rec:
        raise LookupError(f"No market_price_data for {ts}")
    return rec.get("DAM", 0.0), rec.get("RTM", 0.0), rec.get("Market_Purchase", 0.0)


def fetch_battery_status(ts):
    prev = power_db["Battery_Status"].find_one({"Timestamp": ts}, sort=[("Timestamp", -1)])
    if not prev:
        prev = power_db["Battery_Status"].find_one({"Timestamp": {"$lt": ts}}, sort=[("Timestamp", -1)])
    if not prev:
        raise LookupError(f"No Battery_Status before or at {ts}")
    return prev


def in_dsm_window(ts):
    t = ts.time()
    return (time(9, 0) <= t < time(11, 0)) or (time(18, 0) <= t < time(20, 0))


def upsert_battery_status(ts, banked_units, cycle):
    prev = fetch_battery_status(ts)
    prev_units = prev.get("Units_Available", 0.0) or 0.0

    if cycle == "CHARGE":
        new_units = prev_units - banked_units if prev_units > banked_units else 0.0
    elif cycle == "USE":
        new_units = prev_units
    else:
        new_units = prev_units

    power_db["Battery_Status"].update_one(
        {"Timestamp": ts},
        {"$set": {"Units_Available": round(new_units, 3), "Cycle": cycle}},
        upsert=True
    )


def compute_banking_cost(banked_units, total_bd, total_cost, sg, drawl, dam, rtm, market_purchase, ts):
    weighted_avg = round(total_cost / total_bd, 2) if total_bd > 0 else 0.0
    cost = dsm = 0.0
    cycle = "NO CHARGE"

    if banked_units <= 0:
        return cost, dsm, cycle

    if sg > drawl:
        sd = round(sg - drawl, 3)
        if sd > banked_units:
            prev = fetch_battery_status(ts)
            if (prev.get("Units_Available", 0.0) >= banked_units) and not in_dsm_window(ts):
                cycle = "CHARGE"
                cost = 0.0
                upsert_battery_status(ts, banked_units, cycle)
                return cost, dsm, cycle
            else:
                if in_dsm_window(ts):
                    cycle = "USE"
                    dsm = banked_units
                    upsert_battery_status(ts, banked_units, cycle)
                    return cost, dsm, cycle
        else:
            cycle = "NO CHARGE"
            cost = round(weighted_avg * (banked_units - sd), 2)
            if (banked_units - sd) >= total_bd:
                cost = round(weighted_avg * (banked_units - sd) + market_purchase * min(dam, rtm), 2)
            upsert_battery_status(ts, banked_units, cycle)
            return cost, dsm, cycle
    else:
        if total_bd < banked_units:
            cost = round(weighted_avg * total_bd + market_purchase * min(dam, rtm), 2)
        else:
            cost = round(banked_units * weighted_avg, 2)
        upsert_battery_status(ts, banked_units, cycle)

    return cost, dsm, cycle


@router.get("/calculate")
def calculate_banked(start_date: str = Query(..., alias="start_date")):
    try:
        ts = parse_start_timestamp(start_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        banked_units = fetch_banked_units(ts)
        plants = fetch_plant_data(ts)
        sg, drawl = fetch_demand_drawl(ts)
        dam, rtm, market_purchase = fetch_market_prices(ts)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))

    backdown_units = sum(p["backdown_units"] for p in plants)
    backdown_cost = sum(p["backdown_cost"] for p in plants)

    banking_cost, dsm, cycle = compute_banking_cost(
        banked_units, backdown_units, backdown_cost,
        sg, drawl, dam, rtm, market_purchase, ts
    )

    return {
        "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),
        "banked_units": banked_units,
        "total_backdown_units": round(backdown_units, 2),
        "total_backdown_cost": round(backdown_cost, 2),
        "banking_cost": round(banking_cost, 2),
        "DSM": round(dsm, 2),
        "plant_backdown_data": plants,
        "schedule_generation": sg,
        "total_drawl": drawl,
        "dam_rate": dam,
        "rtm_rate": rtm,
        "market_purchase": market_purchase
    }

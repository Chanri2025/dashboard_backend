from flask import Blueprint, jsonify, request, abort
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import os
import math
from datetime import time

from Helpers.helpers import parse_start_timestamp

adjustingAPI = Blueprint('adjusting', __name__)
load_dotenv()

mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
power_db = client["power_casting_new"]


def fetch_adjusted_units(ts):
    rec = power_db["banking_data"].find_one(
        {"Timestamp": ts},
        {"_id": 0, "adjusted_units": 1}
    )
    if not rec:
        raise LookupError(f"No banking_data for {ts}")
    au = rec.get("adjusted_units", 0.0) or 0.0
    return 0.0 if math.isnan(au) else au


def fetch_battery_status(ts):
    """Get the most recent Battery_Status at or before ts."""
    prev = power_db["Battery_Status"].find_one(
        {"Timestamp": ts},
        sort=[("Timestamp", -1)]
    )
    if not prev:
        prev = power_db["Battery_Status"].find_one(
            {"Timestamp": {"$lt": ts}},
            sort=[("Timestamp", -1)]
        )
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
        dc_raw = p.get("DC", 0.0) or 0.0
        sg_raw = p.get("SG", 0.0) or 0.0
        vc_raw = p.get("VC", 0.0) or 0.0

        dc = 0.0 if math.isnan(dc_raw) else dc_raw
        sg = 0.0 if math.isnan(sg_raw) else sg_raw
        vc = round(0.0 if math.isnan(vc_raw) else vc_raw, 2)

        p["DC"], p["SG"], p["VC"] = dc, sg, vc

        bd = round(((dc - sg) * 1000 * 0.25) if dc > sg else 0.0, 2)
        p["backdown_units"] = bd

        cost = round(bd * vc if not math.isnan(bd * vc) else 0.0, 2)
        p["backdown_cost"] = cost

    # sort by VC descending
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


@adjustingAPI.route('/adjustment', methods=['GET'])
def calculate_adjustment():
    # 1) parse & validate timestamp
    raw = request.args.get('start_date')
    try:
        ts = parse_start_timestamp(raw)
    except ValueError as e:
        return abort(400, description=str(e))

    # 2) fetch adjustment_units from banking_data
    try:
        adjustment_unit = fetch_adjusted_units(ts)
    except LookupError as e:
        return abort(404, description=str(e))

    # 3) nothing to do if non‑positive
    if adjustment_unit <= 0:
        return jsonify({
            "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),
            "adjustment_unit": adjustment_unit,
            "adjustment_charges": 0.0,
        }), 200

    # 4) fetch battery status
    status_doc = fetch_battery_status(ts)
    cycle = status_doc.get("Cycle", "").upper()
    available_units = status_doc.get("Units_Available", 0.0)

    # 5) recompute weighted_avg from back‑down data
    plants = fetch_plant_data(ts)
    total_backdown_units = sum(p["backdown_units"] for p in plants)
    total_backdown_cost = sum(p["backdown_cost"] for p in plants)
    weighted_avg = round(
        (total_backdown_cost / total_backdown_units), 2
    ) if total_backdown_units > 0 else 0.0

    # 6) fetch market prices
    dam, rtm, _ = fetch_market_prices(ts)

    # 7) prep rates
    highest_rate = max(weighted_avg, dam, rtm)
    BATTERY_CHARGE_RATE = 4.0

    # 8) compute adjustment_charges
    if cycle == "USE":
        # entire adjustment at highest rate
        adjustment_charges = round(adjustment_unit * highest_rate, 2)
        battery_used = 0.0
        balance_unit = adjustment_unit
    else:
        # consume from battery first
        if adjustment_unit <= available_units:
            battery_used = adjustment_unit
            balance_unit = 0.0
            adjustment_charges = round(battery_used * BATTERY_CHARGE_RATE, 2)
        else:
            battery_used = available_units
            balance_unit = adjustment_unit - available_units
            adjustment_charges = round(
                battery_used * BATTERY_CHARGE_RATE
                + balance_unit * highest_rate,
                2
            )

    # 9) respond
    return jsonify({
        "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),
        "adjustment_unit": adjustment_unit,
        "battery_cycle": cycle,
        "battery_units_available": available_units,
        "battery_units_used": battery_used,
        "balance_units": balance_unit,
        "weighted_avg_rate": weighted_avg,
        "dam_rate": dam,
        "rtm_rate": rtm,
        "highest_rate": highest_rate,
        "battery_charge_rate": BATTERY_CHARGE_RATE,
        "adjustment_charges": adjustment_charges
    }), 200

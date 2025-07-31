from flask import Blueprint, jsonify, request, abort
from pymongo import MongoClient
from dotenv import load_dotenv
import os, math
from datetime import time
from Helpers.helpers import parse_start_timestamp

consolidatedAPI = Blueprint('consolidated', __name__)
load_dotenv()

mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
power_db = client["power_casting_new"]


# ---------- Helpers ----------
def in_dsm_window(ts):
    t = ts.time()
    return (time(9, 0) <= t < time(11, 0)) or (time(18, 0) <= t < time(20, 0))


def fetch_banking_row(ts):
    rec = power_db["banking_data"].find_one({"Timestamp": ts}, {"_id": 0, "banked_units": 1, "adjusted_units": 1})
    if not rec:
        raise LookupError(f"No banking_data for {ts}")
    bu = rec.get("banked_units", 0.0) or 0.0
    au = rec.get("adjusted_units", 0.0) or 0.0
    if math.isnan(bu): bu = 0.0
    if math.isnan(au): au = 0.0
    return bu, au


def fetch_plants(ts):
    cursor = power_db["Plant_Generation"].find(
        {"Timestamp": ts}, {"_id": 0, "Plant_Name": 1, "DC": 1, "SG": 1, "VC": 1}
    )
    plants = []
    for p in cursor:
        dc = 0.0 if math.isnan(p.get("DC", 0.0) or 0.0) else (p.get("DC", 0.0) or 0.0)
        sg = 0.0 if math.isnan(p.get("SG", 0.0) or 0.0) else (p.get("SG", 0.0) or 0.0)
        vc = round(0.0 if math.isnan(p.get("VC", 0.0) or 0.0) else (p.get("VC", 0.0) or 0.0), 2)

        bd_units = round(((dc - sg) * 1000 * 0.25) if dc > sg else 0.0, 2)
        bd_cost = round(bd_units * vc if not math.isnan(bd_units * vc) else 0.0, 2)

        plants.append({
            "Plant_Name": p.get("Plant_Name"),
            "DC": dc, "SG": sg, "VC": vc,
            "backdown_units": bd_units,
            "backdown_cost": bd_cost
        })
    # Keep a copy sorted by VC for MOD
    plants_by_vc = sorted(plants, key=lambda r: r["VC"], reverse=True)
    return plants, plants_by_vc


def fetch_demand_drawl(ts):
    rec = power_db["Demand_Drawl"].find_one(
        {"Timestamp": ts}, {"_id": 0, "Scheduled_Generation": 1, "Drawl": 1}
    )
    if not rec:
        raise LookupError(f"No Demand_Drawl for {ts}")
    sg = rec.get("Scheduled_Generation", 0.0) or 0.0
    dr = rec.get("Drawl", 0.0) or 0.0
    return sg, dr


def fetch_market_prices(ts):
    rec = power_db["market_price_data"].find_one(
        {"Timestamp": ts}, {"_id": 0, "DAM": 1, "RTM": 1, "Market_Purchase": 1}
    )
    if not rec:
        raise LookupError(f"No market_price_data for {ts}")
    dam = rec.get("DAM", 0.0) or 0.0
    rtm = rec.get("RTM", 0.0) or 0.0
    mp = rec.get("Market_Purchase", 0.0) or 0.0
    return dam, rtm, mp


def fetch_battery_status(ts):
    # most recent at or before ts
    doc = power_db["Battery_Status"].find_one({"Timestamp": ts}, sort=[("Timestamp", -1)])
    if not doc:
        doc = power_db["Battery_Status"].find_one({"Timestamp": {"$lt": ts}}, sort=[("Timestamp", -1)])
    if not doc:
        # initialize if missing
        doc = {"Units_Available": 0.0, "Cycle": "NO_CHARGE"}
    return doc


def upsert_battery_status(ts, qty, cycle, *, capacity_limit=None):
    """
    qty: the energy amount for this action (positive).
    Units_Available = free capacity to accept charge (headroom).
      - CHARGE: consume headroom -> headroom -= qty
      - USE   : free headroom    -> headroom += qty
      - else  : unchanged
    capacity_limit: optional maximum headroom, if you model total capacity.
    """
    prev = fetch_battery_status(ts)
    headroom = float(prev.get("Units_Available", 0.0) or 0.0)

    if cycle == "CHARGE":
        headroom = max(headroom - qty, 0.0)
    elif cycle == "USE":
        headroom = headroom + qty
        if capacity_limit is not None:
            headroom = min(headroom, capacity_limit)
    else:
        # NO_CHARGE or anything else -> no change
        pass

    headroom = round(headroom, 3)
    power_db["Battery_Status"].update_one(
        {"Timestamp": ts},
        {"$set": {"Units_Available": headroom, "Cycle": cycle}},
        upsert=True
    )


# ---------- Core calc ----------
def compute_totals(plants, plants_by_vc):
    t_units = round(sum(p["backdown_units"] for p in plants), 2)
    t_cost = round(sum(p["backdown_cost"] for p in plants), 2)
    wav = round((t_cost / t_units), 2) if t_units > 0 else 0.0
    mod = round(plants_by_vc[0]["VC"], 2) if plants_by_vc else 0.0
    return t_units, t_cost, wav, mod


def decide_banking(timestamp, banked_units, scheduled_generation, drawl, weighted_average, mod, dam, rtm,
                   market_purchase,
                   total_backdown_units, units_available_before):
    if banked_units <= 0:
        return {
            "banking_cost": 0.0, "DSM_units": 0.0, "cycle": "NO_CHARGE",
            "units_available_after": units_available_before
        }

    s_d = max(scheduled_generation - drawl, 0.0)  # schedule surplus
    units_after = units_available_before
    banking_cost = 0.0
    dsm_units = 0.0
    cycle = "NO_CHARGE"

    if s_d > 0:
        if s_d >= banked_units:
            if not in_dsm_window(timestamp):
                cycle = "CHARGE"
                banking_cost = 0.0
                # added the banked unit to charge the battery
                upsert_battery_status(timestamp, banked_units, cycle)
            else:
                # dsm all banked units
                dsm_units = banked_units
                cycle = "USE"
                banking_cost = 0.0
                upsert_battery_status(timestamp, banked_units, cycle)
                # cost stays 0
        else:
            # s_d consumes part, rest are "balanced_units"
            balanced_units = round(banked_units - s_d, 3)
            cycle = "NO_CHARGE"
            # your original logic for pricing balanced_units:
            banking_cost = round(weighted_average * balanced_units, 2)
            if balanced_units >= total_backdown_units:
                banking_cost = round(weighted_average * balanced_units + market_purchase * min(dam, rtm), 2)
            upsert_battery_status(timestamp, banked_units, cycle)
    else:
        # sg <= drawl (no surplus)
        if total_backdown_units < banked_units:
            cycle = "NO_CHARGE"
            upsert_battery_status(timestamp, banked_units, cycle)
            banking_cost = round(weighted_average * total_backdown_units + market_purchase * min(dam, rtm), 2)
        else:
            cycle = "NO_CHARGE"
            upsert_battery_status(timestamp, banked_units, cycle)
            banking_cost = round(weighted_average * banked_units, 2)

    return {
        "banking_cost": round(banking_cost, 2),
        "DSM_units": round(dsm_units, 2),
        "cycle": cycle,
        "units_available_after": round(units_after, 3)
    }


def compute_adjustment(timestamp, adjusted_units, mod, dam, rtm,
                       battery_charge_rate=4.0):
    highest_rate = max(mod, dam, rtm)
    battery_status = fetch_battery_status(timestamp)
    units_before = float(battery_status.get("Units_Available", 0.0) or 0.0)
    battery_used = 0.0
    balance_units = 0.0
    adj_cost = 0.0

    # If there is no Adjusted Units
    if adjusted_units <= 0:
        return {
            "adjustment_charges": 0.0,
            "battery_used": 0.0,
            "balance_units": 0.0,
            "units_available_after": units_before,
            "highest_rate": highest_rate,
            "battery_charge_rate": 4.0
        }

    if in_dsm_window(timestamp):
        if adjusted_units < units_before:
            adj_cost = round(adjusted_units * battery_charge_rate, 2)
            # this means that unit will be deducted from battery
            cycle = "USE"
            upsert_battery_status(timestamp, adjusted_units, cycle)
            units_before = units_before - adjusted_units
        else:
            # if enough units not available for deduction from battery
            balance_units = adjusted_units - units_before
            cycle = "USE"
            upsert_battery_status(timestamp, balance_units, cycle)
            adj_cost = units_before * battery_charge_rate + balance_units * highest_rate

    else:
        adj_cost = round(adjusted_units * highest_rate, 2)

    return {
        "adjustment_charges": adj_cost,
        "battery_used": round(battery_used, 3),
        "balance_units": round(balance_units, 3),
        "units_available_after": round(units_before, 3),
        "highest_rate": highest_rate,
        "battery_charge_rate": 4.0
    }


@consolidatedAPI.route('/calculate', methods=['GET'])
def calculate_consolidated():
    raw = request.args.get('start_date')
    try:
        ts = parse_start_timestamp(raw)
    except ValueError as e:
        return abort(400, description=str(e))

    try:
        # Getting banked unit and adjusted units
        banked_units, adjusted_units = fetch_banking_row(ts)
        # Getting plants data
        plants, plants_by_vc = fetch_plants(ts)
        # Getting SG and Drawl
        sg_sched, drawl = fetch_demand_drawl(ts)
        # Getting DAM, RTM, Market Purchase
        dam, rtm, market_purchase = fetch_market_prices(ts)
        # Getting the Battery Details
        battery_details = fetch_battery_status(ts)
    except LookupError as e:
        return abort(404, description=str(e))

    total_backdown_units, total_backdown_cost, weighted_average, mod = compute_totals(plants, plants_by_vc)
    units_left_to_charge = float(battery_details.get("Units_Available", 0.0) or 0.0)

    # 1) Banking (may change battery)
    bank = decide_banking(
        ts, banked_units, sg_sched, drawl, weighted_average, mod, dam, rtm, market_purchase,
        total_backdown_units, units_left_to_charge
    )

    # 2) Adjustment (uses post-banking battery)
    adj = compute_adjustment(
        ts, adjusted_units, bank["units_available_after"], mod, dam, rtm,
    )

    return jsonify({
        "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),

        # Inputs
        "banked_units": banked_units,
        "adjusted_units": adjusted_units,
        "schedule_generation": sg_sched,
        "total_drawl": drawl,
        "dam_rate": dam,
        "rtm_rate": rtm,
        "market_purchase": market_purchase,

        # Plant & totals
        "plant_backdown_data": plants,
        "total_backdown_units": total_backdown_units,
        "total_backdown_cost": total_backdown_cost,
        "weighted_avg_rate": weighted_average,
        "marginal_rate": mod,
        "highest_rate": max(mod, dam, rtm),

        # Banking result
        "banking_cost": bank["banking_cost"],
        "DSM": bank["DSM_units"],
        "banking_cycle": bank["cycle"],

        # Adjustment result
        "adjustment_charges": adj["adjustment_charges"],
        "battery_units_used_for_adjustment": adj["battery_used"],
        "adjustment_balance_units": adj["balance_units"],
        "battery_charge_rate": adj["battery_charge_rate"],

        # Battery snapshot
        "battery_units_before": units_left_to_charge,
        "battery_units_after_banking": bank["units_available_after"],
        "battery_units_after_adjustment": adj["units_available_after"]
    }), 200

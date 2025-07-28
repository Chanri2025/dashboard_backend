from flask import Blueprint, jsonify, request, abort
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import math
from datetime import timedelta, time

from Helpers.helpers import parse_start_timestamp

bankingAPI = Blueprint('banking', __name__)
load_dotenv()

mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
power_db = client["power_casting_new"]


def fetch_banked_units(ts):
    rec = power_db["banking_data"].find_one(
        {"Timestamp": ts}, {"_id": 0, "banked_units": 1}
    )
    if not rec:
        raise LookupError(f"No banking_data for {ts}")
    bu = rec.get("banked_units", 0.0) or 0.0
    return 0.0 if math.isnan(bu) else bu


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

    return plants


def fetch_demand_drawl(ts):
    rec = power_db["Demand_Drawl"].find_one(
        {"Timestamp": ts}, {"_id": 0, "Scheduled_Generation": 1, "Drawl": 1}
    )
    if not rec:
        raise LookupError(f"No Demand_Drawl for {ts}")
    sg = rec.get("Scheduled_Generation", 0.0) or 0.0
    dr = rec.get("Drawl", 0.0) or 0.0
    return (sg, dr)


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
    return (dam, rtm, market_purchase)


def fetch_battery_status(ts):
    prev = ts - timedelta(minutes=15)
    rec = power_db["Battery_Status"].find_one(
        {"Timestamp": prev},
        sort=[("Timestamp", -1)]
    )
    if not rec:
        rec = power_db["Battery_Status"].find_one(
            {"Timestamp": {"$lt": ts}},
            sort=[("Timestamp", -1)]
        )
    if not rec:
        raise LookupError(f"No Battery_Status before {ts}")
    return rec


def in_dsm_window(ts):
    t = ts.time()
    return (time(9, 0) <= t < time(11, 0)) or (time(18, 0) <= t < time(20, 0))


def compute_banking_cost(
        banked, total_backdown_units, total_backdown_cost,
        scheduled_generation, drawl, dam, rtm, market_purchase, ts
):
    dsm = 0.0
    if banked <= 0:
        return 0.0, dsm

    weighted_avg = (total_backdown_cost / total_backdown_units) if total_backdown_units > 0 else 0.0
    print("Banking Data: ", banked)
    print("Weighted Average Cost: ", weighted_avg)
    print("SG: ", scheduled_generation)
    print("Drawl: ", drawl)
    print("Schedule > Drawl", scheduled_generation > drawl)
    if scheduled_generation > drawl:
        sd = scheduled_generation - drawl
        print("SG - Drawl: ", sd)
        print("Schedule - Drawl (S-D)> Banking", sd > banked)
        Balanced_Unit = banked - sd
        print("Balanced Units: ", Balanced_Unit)
        print ("Balanced Units < Total Backdown Units", Balanced_Unit < total_backdown_units)
        if sd > banked:
            batt = fetch_battery_status(ts)
            print("Battery Status: ", batt)
            if in_dsm_window(ts):
                dsm = banked
                return banked, dsm
        else:
            if Balanced_Unit < total_backdown_units:
                print("Banked Cost: weighted_avg * Balanced_Unit: ",weighted_avg * Balanced_Unit)
                return weighted_avg * Balanced_Unit, dsm
            else:
                cost = weighted_avg * Balanced_Unit + market_purchase * min(dam, rtm)
                print("Banked Cost: weighted_avg * Balanced_Unit + market_purchase * min(dam, rtm): ", cost)
                return cost, dsm


    if scheduled_generation <= drawl and total_backdown_units < banked:
        print('''
            Backdown (BD) < Banking and Schedule > Drawl
            ''')
        cost = weighted_avg * total_backdown_units + market_purchase * min(dam, rtm)
        print("Banking Cost: weighted_avg * total_backdown_units + market_purchase * min(dam, rtm): ", cost)
    else:
        # diff = total_backdown_units - banked
        cost = banked * weighted_avg
        print("Cost: banked * weighted_avg: ",cost)

    return cost, dsm


@bankingAPI.route('/calculate', methods=['GET'])
def calculate_banked():
    raw = request.args.get('start_date')
    try:
        ts = parse_start_timestamp(raw)
    except ValueError as e:
        return abort(400, description=str(e))

    try:
        banked = fetch_banked_units(ts)
        plants = fetch_plant_data(ts)
        sg, dr = fetch_demand_drawl(ts)
        dam, rtm, mpur = fetch_market_prices(ts)
    except LookupError as e:
        return abort(404, description=str(e))

    total_units = sum(p["backdown_units"] for p in plants)
    total_cost = sum(p["backdown_cost"] for p in plants)
    print("TimeStamp: ", raw)
    print("Backdown Units: ",total_units)
    print("Backdown Cost: ", total_cost)
    banking_cost, dsm = compute_banking_cost(
        banked, total_units, total_cost,
        sg, dr, dam, rtm, mpur, ts
    )

    return jsonify({
        "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),
        "banked_units": banked,
        "total_backdown_units": round(total_units, 2),
        "total_backdown_cost": round(total_cost, 2),
        "banking_cost": round(banking_cost, 2),
        "DSM": round(dsm, 2),
        "plant_backdown_data": plants
    }), 200

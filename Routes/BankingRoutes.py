from flask import Blueprint, jsonify, request, abort
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import os
import math
from datetime import time

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
    return sg, dr


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


def in_dsm_window(ts):
    t = ts.time()
    return (time(9, 0) <= t < time(11, 0)) or (time(18, 0) <= t < time(20, 0))


def upsert_battery_status(ts, banked_units, cycle):
    """
    Compute new Units_Available and upsert Battery_Status.
    If cycle == "USE": subtract banked_units.
    If cycle == "CHARGE": add banked_units.
    """
    prev = fetch_battery_status(ts)
    prev_units = prev.get("Units_Available", 0.0) or 0.0

    if cycle == "CHARGE":
        if prev_units > banked_units:
            new_units = prev_units - banked_units
            print(f"Charging battery by {banked_units} units, units avaiable {new_units}")
        else:
            new_units = prev_units - (banked_units-prev_units)
            print(f"Charging battery by {prev_units} units, units avaiable {new_units}, {(banked_units-prev_units)} going to DSM")
    elif cycle=="USE":
        new_units = prev_units
        print(f"No Charging taking place, battery is in use state.")
    else:
        new_units = prev_units
        print(f"No Charging taking place")

    new_units = round(new_units, 3)
    power_db["Battery_Status"].update_one(
        {"Timestamp": ts},
        {"$set": {"Units_Available": new_units, "Cycle": cycle}},
        upsert=True
    )
    print(f"Upserted Battery_Status @ {ts}: Cycle={cycle}, Units_Available={new_units}")


def compute_banking_cost(
        banked_units, total_backdown_units, total_cost,
        scheduled_generation, drawl, dam, rtm, market_purchase, ts
):
    """
    Returns (cost, dsm_units, cycle)
    """
    print("Banking Data: ", banked_units)
    weighted_avg = round((total_cost / total_backdown_units), 2) if total_backdown_units > 0 else 0.0
    print("Weighted Average Cost: ", weighted_avg)
    print("SG: ", scheduled_generation)
    print("Drawl: ", drawl)
    print("Schedule > Drawl:", scheduled_generation > drawl)

    cost = 0.0
    dsm = 0.0
    cycle = "NO CHARGE"

    if banked_units <= 0:
        return cost, dsm, cycle

    if scheduled_generation > drawl:
        sd = round(scheduled_generation - drawl, 3)
        print("SG - Drawl: ", sd)
        balanced_unit = banked_units - sd
        print("(SG - Drawl) > Banking Units: ", sd > banked_units)


        if sd > banked_units:
            print("SD > banked, checking for charge scenario")
            prev = fetch_battery_status(ts)
            prev_units = prev.get("Units_Available", 0.0) or 0.0
            print("Previous Units Available:", prev_units)

            # Charge if enough capacity and outside DSM window
            if (prev_units >= banked_units) and (not in_dsm_window(ts)):
                cycle = "CHARGE"
                cost = 0.0
                print("Entering CHARGE cycle, cost set to 0")
                upsert_battery_status(ts, banked_units, cycle)
                return cost,dsm,cycle
            else:
                if in_dsm_window(ts):
                    cycle = "USE"
                    dsm = banked_units
                    cost = 0.0
                    print("In DSM window, using DSM:", dsm)
                    upsert_battery_status(ts,banked_units, cycle)
                return cost, dsm, cycle
        else:
            # NO CHARGE
            print("Balanced Units: ", balanced_unit)
            cycle = "NO CHARGE"
            print(cycle)
            cost = round(weighted_avg * balanced_unit, 2)
            print("Banked Cost: (balanced_unit * weighted_avg):", cost)
            if balanced_unit >= total_backdown_units:
                cost = round(weighted_avg * balanced_unit + market_purchase * min(dam, rtm), 2)
                print("Banked cost: weighted_avg * balanced_unit + market_purchase * min(dam, rtm):", cost)
            upsert_battery_status(ts, banked_units, cycle)
            return cost,dsm,cycle

    else:
        if total_backdown_units < banked_units:
            cycle = "NO CHARGE"
            cost = round(weighted_avg * total_backdown_units + market_purchase * min(dam, rtm), 2)
            print("Banked cost: Backdown < Banking and SG<=Drawl, cost:", cost)
            upsert_battery_status(ts, banked_units, cycle)
            return cost, dsm, cycle
        else:
            cycle = "NO CHARGE"
            cost = round(banked_units * weighted_avg, 2)
            upsert_battery_status(ts, banked_units, cycle)
            print("Banked cost: (banked * weighted_avg):", cost)

    return cost, dsm, cycle


@bankingAPI.route('/calculate', methods=['GET'])
def calculate_banked():
    raw = request.args.get('start_date')
    try:
        ts = parse_start_timestamp(raw)
    except ValueError as e:
        return abort(400, description=str(e))

    try:
        banked_units = fetch_banked_units(ts)
        plants = fetch_plant_data(ts)
        scheduled_generation, drawl = fetch_demand_drawl(ts)
        dam, rtm, marketpurchase = fetch_market_prices(ts)
    except LookupError as e:
        return abort(404, description=str(e))

    backdown_units = sum(p["backdown_units"] for p in plants)
    backdown_cost = sum(p["backdown_cost"] for p in plants)

    print("Timestamp: ", raw)
    print("Backdown Units:", backdown_units)
    print("Backdown Cost: ", backdown_cost)
    print("Scheduled_Generation:", scheduled_generation)
    print("Drawl:", drawl)

    banking_cost, dsm, cycle = compute_banking_cost(
        banked_units, backdown_units, backdown_cost,
        scheduled_generation, drawl, dam, rtm, marketpurchase, ts
    )

    return jsonify({
        "Timestamp": ts.strftime("%Y-%m-%d %H:%M"),
        "banked_units": banked_units,
        "total_backdown_units": round(backdown_units, 2),
        "total_backdown_cost": round(backdown_cost, 2),
        "banking_cost": round(banking_cost, 2),
        "DSM": round(dsm, 2),
        "plant_backdown_data": plants,
        "schedule_generation": scheduled_generation,
        "total_drawl": drawl,
        "dam_rate": dam,
        "rtm_rate": rtm,
        "market_purchase":marketpurchase

    }), 200

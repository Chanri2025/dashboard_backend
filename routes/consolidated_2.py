from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import time, timedelta
from dotenv import load_dotenv
from bisect import bisect_left
from collections import OrderedDict
import os, math

from Helpers.helpers import parse_start_timestamp  # your existing helper

router = APIRouter()
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
power_db = client["power_casting_new"]

# --- target collection for consolidated records ---
bank_adj_coll = power_db["Banking-Adjust-consolidated"]

# --- helpful compound indexes (create if missing, safe to call repeatedly) ---
power_db["Plant_Generation"].create_index([("Timestamp", ASCENDING), ("VC", ASCENDING)])
power_db["banking_data"].create_index([("Timestamp", ASCENDING)])
power_db["Demand_Drawl"].create_index([("Timestamp", ASCENDING)])
power_db["market_price_data"].create_index([("Timestamp", ASCENDING)])
power_db["Battery_Status"].create_index([("Timestamp", ASCENDING)])

# ---------- O(log n) prefix cache (LRU) ----------
# Key: datetime timestamp; Value: dict with prefix arrays and plant lists
_PREFIX_CACHE_MAX = 256
_prefix_cache: OrderedDict = OrderedDict()  # timestamp -> {"vc":[], "bu":[], "cum_units":[], "cum_cost":[], "plants_asc":[], "plants_desc":[]}


def _cache_put(ts, entry):
    # Simple LRU
    _prefix_cache[ts] = entry
    _prefix_cache.move_to_end(ts)
    if len(_prefix_cache) > _PREFIX_CACHE_MAX:
        _prefix_cache.popitem(last=False)


def _cache_get(ts):
    if ts in _prefix_cache:
        _prefix_cache.move_to_end(ts)
        return _prefix_cache[ts]
    return None


# ---------- Helpers ----------
def calculate_weighted_average_for_quantum_prefix(q, ts):
    cached = _cache_get(ts)
    if not cached:
        raise LookupError("Prefix cache not prepared for timestamp")

    vc = cached["vc"]
    cum_units = cached["cum_units"]
    cum_cost = cached["cum_cost"]

    if q <= 0:
        return 0.0, 0.0, 0.0

    k = bisect_left(cum_units, q)

    # Case 1: q smaller than first block
    if k == 0:
        total_cost = q * vc[0]

    # Case 2: q larger than all available units → clamp to last
    elif k >= len(cum_units):
        full_units = cum_units[-1]
        full_cost = cum_cost[-1]
        extra = q - full_units
        total_cost = full_cost + extra * vc[-1]  # use last VC as marginal rate

    # Case 3: normal in-between
    else:
        full_units = cum_units[k - 1]
        full_cost = cum_cost[k - 1]
        partial = q - full_units
        total_cost = full_cost + partial * vc[k]

    weighted_avg = round(total_cost / q, 2)
    return weighted_avg, round(total_cost, 2), round(q, 2)


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


def safe_float(val, default=0.0):
    try:
        f = float(val)
        return f if not math.isnan(f) else default
    except (ValueError, TypeError):
        return default


def fetch_plants_prepare_prefix(ts):
    """
    Fetch plants for timestamp ts with Mongo server-side sort by VC ASC (cheapest-first),
    compute backdown_units/cost ONLY for Thermal plants (via Plant_Fuel),
    build prefix sums, and cache both ASC + DESC lists.
    """
    # --- fetch fuel types (cached globally for efficiency) ---
    fuel_map = {}
    cursor_fuel = power_db["Plant_Fuel"].find({}, {"_id": 0, "Plant_Name": 1, "Fuel_Type": 1})
    for f in cursor_fuel:
        fuel_map[f["Plant_Name"]] = f.get("Fuel_Type", "")

    cursor = power_db["Plant_Generation"].find(
        {"Timestamp": ts},
        {"_id": 0, "Plant_Name": 1, "DC": 1, "SG": 1, "VC": 1}
    ).sort("VC", ASCENDING)

    plants_asc = []
    for p in cursor:
        name = p.get("Plant_Name")
        dc = safe_float(p.get("DC", 0.0))
        sg = safe_float(p.get("SG", 0.0))
        vc = round(safe_float(p.get("VC", 0.0)), 2)

        # ✅ Backdown logic by plant type
        if fuel_map.get(name) == "Thermal":
            if dc > sg and sg > 0:
                bd_units = round((dc - sg) * 1000 * 0.25, 2)
            else:
                bd_units = 0.0
        else:
            if dc > sg:
                bd_units = round((dc - sg) * 1000 * 0.25, 2)
            else:
                bd_units = 0.0

        bd_cost = round(bd_units * vc if not math.isnan(bd_units * vc) else 0.0, 2)

        plants_asc.append({
            "Plant_Name": name,
            "Fuel_Type": fuel_map.get(name, "Unknown"),
            "DC": dc, "SG": sg, "VC": vc,
            "backdown_units": bd_units,
            "backdown_cost": bd_cost
        })

    if not plants_asc:
        return [], []

    # --- prefix sums only over Thermal (bd_units=0 for others, so safe) ---
    vc = [row["VC"] for row in plants_asc]
    bu = [row["backdown_units"] for row in plants_asc]
    cum_units, cum_cost = [], []
    u = c = 0.0
    for i in range(len(plants_asc)):
        u += bu[i]
        c += bu[i] * vc[i]
        cum_units.append(round(u, 6))
        cum_cost.append(round(c, 6))

    plants_desc = list(reversed(plants_asc))

    _cache_put(ts, {
        "vc": vc,
        "bu": bu,
        "cum_units": cum_units,
        "cum_cost": cum_cost,
        "plants_asc": plants_asc,
        "plants_desc": plants_desc
    })

    return plants_asc, plants_desc


def fetch_mod_from_demand_output(ts):
    rec = client["powercasting"]["Demand_Output"].find_one(
        {"Timestamp": ts},
        {"_id": 0, "Last_Price": 1}
    )
    if not rec:
        raise LookupError(f"No Demand_Output for {ts}")
    return round(rec.get("Last_Price", 0.0) or 0.0, 2)


def compute_totals(plants_list, timestamp):
    t_units = round(sum(p["backdown_units"] for p in plants_list), 2)
    t_cost = round(sum(p["backdown_cost"] for p in plants_list), 2)
    wav = round((t_cost / t_units), 2) if t_units > 0 else 0.0

    # ✅ MOD from Demand_Output.Last_Price
    try:
        mod = fetch_mod_from_demand_output(timestamp)
    except LookupError:
        # fallback: keep old logic
        mod = round(plants_list[0]["VC"], 2) if plants_list else 0.0

    return t_units, t_cost, wav, mod


def fetch_battery_status(ts):
    """
    Strictly before ts; if none, initialize synthetic 'previous block' document.
    """
    doc = power_db["Battery_Status"].find_one(
        {"Timestamp": {"$lt": ts}},
        sort=[("Timestamp", DESCENDING)]
    )
    if not doc:
        doc = {
            "Timestamp": ts - timedelta(minutes=15),
            "Units_Available": 2823529.412,
            "Cycle": "NO_CHARGE"
        }
    return doc


def upsert_battery_status(ts, qty, cycle, *, capacity_limit=None):
    """
    qty: positive energy amount for this action.
    Units_Available = free capacity/headroom.
      - CHARGE: headroom -= qty
      - USE   : headroom += qty
      - NO_CHARGE: unchanged
    """
    prev = fetch_battery_status(ts)
    headroom = float(prev.get("Units_Available", 0.0) or 0.0)

    if cycle == "CHARGE":
        headroom = max(headroom - qty, 0.0)
    elif cycle == "USE":
        headroom = headroom + qty
    else:
        pass

    if capacity_limit is not None:
        headroom = min(headroom, capacity_limit)

    headroom = round(headroom, 3)
    power_db["Battery_Status"].update_one(
        {"Timestamp": ts},
        {"$set": {"Units_Available": headroom, "Cycle": cycle}},
        upsert=True
    )


def allocate_used_for_quantum_desc(ts, quantum):
    """
    Populate used_for_quantum per-plant (single O(n) pass) and return list sorted by VC DESC.
    Uses cached plants ASC, fills usage cheapest-first, then returns DESC for response.
    """
    cached = _cache_get(ts)
    if not cached:
        raise LookupError("Prefix cache not prepared for timestamp")

    # Work on a copy to not mutate cache
    plants_asc = [dict(p) for p in cached["plants_asc"]]
    remaining = max(0.0, float(quantum or 0.0))
    for p in plants_asc:
        avail = p.get("backdown_units", 0.0) or 0.0
        use = min(avail, remaining)
        p["used_for_quantum"] = round(use, 3)
        remaining -= use
        if remaining <= 0:
            # Fill zeros for the rest if any
            pass
    # Return DESC order
    plants_desc = list(reversed(plants_asc))
    return plants_desc


def decide_banking(timestamp, banked_units, scheduled_generation, drawl,
                   weighted_average_mod, mod, dam, rtm,
                   market_purchase_input,
                   total_backdown_units, total_backdown_cost,
                   units_available_before):
    """
    Uses O(log n) prefix sums to compute weighted average costs.
    Also returns plants_with_usage (DESC) for the response.
    """
    s_d = max(scheduled_generation - drawl, 0.0)  # schedule surplus
    units_after = units_available_before
    banking_cost = 0.0
    market_purchase = 0.0
    dsm_units = 0.0
    cycle = "NO_CHARGE"

    # default plants_with_usage = zeros allocated (but DESC sorted for response)
    plants_with_usage = allocate_used_for_quantum_desc(timestamp, 0.0)

    if banked_units <= 0:
        upsert_battery_status(timestamp, 0, cycle)
        return {
            "banking_cost": 0.0,
            "DSM_units": 0.0,
            "cycle": cycle,
            "units_available_after": units_available_before,
            "weighted_average": round(weighted_average_mod, 2),
            "market_purchase": 0.0,
            "plants_with_usage": plants_with_usage
        }

    if s_d > 0:
        if s_d >= banked_units:
            if not in_dsm_window(timestamp):
                cycle = "CHARGE"
                banking_cost = 0.0
                if units_after == 0:
                    # all go to DSM (nothing to charge)
                    dsm_units = banked_units
                    upsert_battery_status(timestamp, 0, cycle)
                elif units_available_before > banked_units:
                    # all banked_units go to battery
                    dsm_units = 0.0
                    upsert_battery_status(timestamp, banked_units, cycle)
                    units_after = units_available_before - banked_units
                else:
                    # partial: some go to battery, rest to DSM
                    dsm_units = banked_units - units_available_before
                    upsert_battery_status(timestamp, units_available_before, cycle)
                    units_after = 0.0
            else:
                # DSM all banked units during DSM window
                dsm_units = banked_units
                cycle = "NO_CHARGE"
                banking_cost = 0.0
                upsert_battery_status(timestamp, banked_units, cycle)
            # for display, allocation is not actually used here; keep zeros
        else:
            # s_d consumes part; remaining are "balanced_units"
            balanced_units = round(banked_units - s_d, 3)
            cycle = "NO_CHARGE"
            # O(log n) weighted average using prefix
            wavg, tot_bd_cost_for_balanced, total_units_used = calculate_weighted_average_for_quantum_prefix(
                balanced_units, timestamp
            )
            banking_cost = round(tot_bd_cost_for_balanced, 2)
            total_backdown_units_used = total_units_used

            if balanced_units >= total_backdown_units_used:
                # Extra from market
                market_purchase = balanced_units - total_backdown_units_used
                banking_cost = round(tot_bd_cost_for_balanced + market_purchase * min(dam, rtm), 2)

            upsert_battery_status(timestamp, banked_units, cycle)
            # Provide per-plant usage for the entire banked_units (for UI visibility)
            plants_with_usage = allocate_used_for_quantum_desc(timestamp, balanced_units)
            weighted_average_mod = wavg
    else:
        # No surplus: sg <= drawl
        if total_backdown_units < banked_units:
            # Need market purchase for the shortfall
            cycle = "NO_CHARGE"
            upsert_battery_status(timestamp, banked_units, cycle)
            market_purchase = banked_units - total_backdown_units
            # total_backdown_cost corresponds to using all available backdown
            banking_cost = round(total_backdown_cost + market_purchase * min(dam, rtm), 2)
            plants_with_usage = allocate_used_for_quantum_desc(timestamp, total_backdown_units)
        else:
            # Sufficient backdown available; cost is weighted average * banked_units
            cycle = "NO_CHARGE"
            upsert_battery_status(timestamp, banked_units, cycle)
            wavg, tot_cost, _ = calculate_weighted_average_for_quantum_prefix(banked_units, timestamp)
            banking_cost = round(wavg * banked_units, 2)
            weighted_average_mod = wavg
            plants_with_usage = allocate_used_for_quantum_desc(timestamp, banked_units)

    return {
        "banking_cost": round(banking_cost, 2),
        "DSM_units": round(dsm_units, 2),
        "cycle": cycle,
        "units_available_after": round(units_after, 3),
        "weighted_average": round(weighted_average_mod, 2),
        "market_purchase": round(market_purchase, 2),
        "plants_with_usage": plants_with_usage
    }


def compute_adjustment(timestamp, adjusted_units, mod, dam, rtm,
                       battery_charge_rate=4.0):
    highest_rate = max(mod, dam, rtm)
    battery_status = fetch_battery_status(timestamp)
    units_before = float(battery_status.get("Units_Available", 0.0) or 0.0)
    battery_used = 0.0
    balance_units = 0.0
    battery_units = 2823529.412
    adj_cost = 0.0

    if units_before > 0:
        battery_units = battery_units - units_before

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
        if adjusted_units < battery_units:
            adj_cost = round(adjusted_units * battery_charge_rate, 2)
            cycle = "USE"
            upsert_battery_status(timestamp, adjusted_units, cycle)
            units_before = adjusted_units + units_before
            battery_used = adjusted_units
        else:
            balance_units = adjusted_units - battery_units
            cycle = "USE"
            upsert_battery_status(timestamp, battery_units, cycle)
            adj_cost = battery_units * battery_charge_rate + balance_units * highest_rate
            units_before = battery_units + units_before
            battery_used = battery_units
    else:
        adj_cost = round(adjusted_units * highest_rate, 2)
        cycle = "NO_CHARGE"
        upsert_battery_status(timestamp, adjusted_units, cycle)

    return {
        "adjustment_charges": round(adj_cost, 2),
        "battery_used": round(battery_used, 3),
        "balance_units": round(balance_units, 3),
        "units_available_after": round(units_before, 3),
        "highest_rate": highest_rate,
        "battery_charge_rate": 4.0
    }


# --- FastAPI route ---
@router.get("/calculate/v2")
async def calculate_consolidated(start_date: str = Query(..., alias="start_date")):
    # Parse timestamp
    try:
        timestamp = parse_start_timestamp(start_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Fetch & prepare (includes prefix caching and both ASC/DESC lists)
    plants_asc, plants_desc = fetch_plants_prepare_prefix(timestamp)

    try:
        banked_units, adjusted_units = fetch_banking_row(timestamp)
        scheduled_generation, drawl = fetch_demand_drawl(timestamp)
        dam, rtm, market_purchase_in = fetch_market_prices(timestamp)
        battery_details = fetch_battery_status(timestamp)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))

    # Totals on DESC (for MOD, etc.)
    total_backdown_units, total_backdown_cost, weighted_average_base, mod = compute_totals(plants_desc, timestamp)
    units_left_to_charge = float(battery_details.get("Units_Available", 0.0) or 0.0)

    # Banking + allocation
    bank = decide_banking(
        timestamp, banked_units, scheduled_generation, drawl,
        weighted_average_base, mod, dam, rtm, market_purchase_in,
        total_backdown_units, total_backdown_cost,
        units_left_to_charge
    )

    # Adjustment
    adj = compute_adjustment(
        timestamp, adjusted_units, mod, dam, rtm,
    )

    # Always return plant_backdown_data sorted by VC Ascending (with used_for_quantum present)
    plant_rows_asc = sorted(bank.get("plants_with_usage") or plants_desc, key=lambda r: r["VC"])

    result = {
        "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M"),

        "banked_units": round(banked_units, 3),
        "adjusted_units": round(adjusted_units, 3),
        "schedule_generation": round(scheduled_generation, 3),
        "total_drawl": round(drawl, 3),
        "dam_rate": round(dam, 2),
        "rtm_rate": round(rtm, 2),

        "plant_backdown_data": plant_rows_asc,
        "total_backdown_units": round(total_backdown_units, 3),
        "total_backdown_cost": round(total_backdown_cost, 2),
        "weighted_avg_rate": bank["weighted_average"],
        "MOD_rate": mod,
        "highest_rate": max(mod, dam, rtm),

        "banking_cost": bank["banking_cost"],
        "DSM": bank["DSM_units"],
        "banking_cycle": bank["cycle"],

        "adjustment_charges": adj["adjustment_charges"],
        "battery_units_used_for_adjustment": adj["battery_used"],
        "market_purchase": round(adj["balance_units"] + bank["market_purchase"], 3),
        "battery_charge_rate": adj["battery_charge_rate"],

        "battery_units_before_banking": units_left_to_charge,
        "battery_units_available_after_banking": bank["units_available_after"],
        "units_used_to_charge": round(units_left_to_charge - bank["units_available_after"], 3),
        "units_used_to_adjust": round(adj["units_available_after"] - bank["units_available_after"], 3),
        "battery_units_after_adjustment": adj["units_available_after"]
    }

    # Persist document with real datetime for index; keep a string mirror for readability
    mongo_doc = {
        **result,
        "Timestamp": timestamp,
        "Timestamp_str": result["Timestamp"],
        "_meta": {"source": "calculate_consolidated"},
    }

    try:
        bank_adj_coll.update_one(
            {"Timestamp": timestamp},
            {"$set": mongo_doc, "$currentDate": {"updated_at": True}},
            upsert=True
        )
    except Exception as e:
        return JSONResponse(
            status_code=207,
            content={
                "warning": f"Computed but failed to persist to Banking-Adjust-consolidated: {str(e)}",
                **result
            }
        )

    return JSONResponse(content=result)

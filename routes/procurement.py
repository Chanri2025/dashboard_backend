import logging

from fastapi import APIRouter, HTTPException, Query
from collections import OrderedDict
from fastapi.responses import JSONResponse
from fastapi import Request
from datetime import timedelta
from functools import lru_cache
from datetime import datetime
from typing import Dict, Any, List, Union
import os

from dotenv import load_dotenv
from pymongo import MongoClient
from mysql.connector.pooling import MySQLConnectionPool

from utils import date_utils as du
from utils.mongo_helpers import to_float

router = APIRouter()
load_dotenv()

# ───────────────────── DB clients (global, reused) ─────────────────────
db_names = (os.getenv("DB_NAMES") or "").split(",")
db_name = db_names[1].strip() if len(db_names) > 1 else (db_names[0].strip() if db_names else "")

db_config = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "database": db_name,
}
mysql_pool = MySQLConnectionPool(pool_name="pc_pool", pool_size=10, **db_config)

mongo_uri = os.getenv("MONGO_URI")
_mclient = MongoClient(mongo_uri)
mdb = _mclient["powercasting"]

Demand = mdb["Demand"]
Banking = mdb["Banking_Data"]
IEX_Gen = mdb["IEX_Generation"]
MustRunPred = mdb["mustrunplantconsumption"]
DemandOutput = mdb["Demand_Output_Approval"]


# ───────────────────── cached lookups ─────────────────────
@lru_cache(maxsize=1)
def _load_must_run_plants() -> List[Dict[str, Any]]:
    conn = mysql_pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
                    SELECT name,
                           Code,
                           Rated_Capacity,
                           PAF,
                           PLF,
                           Type,
                           Technical_Minimum,
                           Aux_Consumption,
                           Variable_Cost,
                           Max_Power,
                           Min_Power
                    FROM plant_details
                    WHERE Type = 'Must run'
                    ORDER BY Variable_Cost
                    """)
        rows = cur.fetchall()
        return rows
    finally:
        cur.close();
        conn.close()


@lru_cache(maxsize=12)
def _load_other_plants(month_col: str) -> List[Dict[str, Any]]:
    conn = mysql_pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT pd.name, pd.Code, pd.Rated_Capacity, pd.PAF, pd.PLF, pd.Type,
                   pd.Technical_Minimum, pd.Aux_Consumption, pd.Variable_Cost,
                   pd.Max_Power, pd.Min_Power
            FROM plant_details pd
            JOIN paf_details pfd ON pd.Code=pfd.Code
            WHERE pd.Type='Other' AND pfd.`{month_col}`='Y'
            ORDER BY pd.Variable_Cost ASC
        """)
        rows = cur.fetchall()
        return rows
    finally:
        cur.close();
        conn.close()


@lru_cache(maxsize=1)
def _load_backdown_table() -> List[Dict[str, float]]:
    conn = mysql_pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT Start_Load, End_Load, SHR, Aux_Consumption FROM back_down_table")
        rows = cur.fetchall()
        return [
            {"lower": r["Start_Load"], "upper": r["End_Load"], "SHR": r["SHR"], "Aux_Consumption": r["Aux_Consumption"]}
            for r in rows
        ]
    finally:
        cur.close();
        conn.close()


# ───────────────────── helpers ─────────────────────
def _map_and_calculate(alloc: Dict[str, Any], plant_dict: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    p = plant_dict.get(alloc["plant_code"], {})
    rated = float(p.get("Rated_Capacity", 0.0))
    paf = float(p.get("PAF", 0.0))
    aux = float(p.get("Aux_Consumption", 0.0))
    var_cost = float(p.get("Variable_Cost", 0.0))

    denom = rated * 1000.0 * 0.25 * paf * (1.0 - aux)
    plf = 0.0 if denom <= 0 else alloc["allocated_gen"] / denom

    gen = alloc["allocated_gen"]
    max_energy = alloc["max_gen"]
    net_cost = gen * var_cost

    return {
        "plant_name": p.get("name", "Unknown"),
        "plant_code": alloc["plant_code"],
        "rated_capacity": rated,
        "paf": round(paf, 4),
        "Aux_Consumption": aux,
        "plf": round(plf, 6),
        "Variable_Cost": var_cost,
        "max_power": round(alloc["max_gen"], 3),
        "min_power": round(alloc["min_gen"], 3),
        "generated_energy": round(gen, 3),
        "energy_not_taken": round(max_energy - gen, 3),
        "net_cost": round(net_cost, 2),
    }


def _allocate_generation(plants: List[Dict[str, Any]], net_demand: float,
                         backdown_table: List[Dict[str, float]]) -> Dict[str, Union[float, List[Any]]]:
    if net_demand <= 0:
        raise ValueError("Net demand must be greater than zero")

    sorted_plants = sorted(plants, key=lambda p: p["Variable_Cost"])
    allocation, total_alloc = [], 0.0

    for plant in sorted_plants:
        max_p = float(plant["Max_Power"])
        if max_p <= 0:
            continue
        min_p = float(plant["Min_Power"])
        rem = net_demand - total_alloc
        if rem <= 0:
            break

        if rem <= max_p:
            alloc_val = max(min_p, rem)
            allocation.append({"plant_code": plant["Code"], "allocated_gen": alloc_val,
                               "min_gen": min_p, "max_gen": max_p, "Type": plant["Type"]})
            total_alloc += alloc_val
            break

        allocation.append({"plant_code": plant["Code"], "allocated_gen": max_p,
                           "min_gen": min_p, "max_gen": max_p, "Type": plant["Type"]})
        total_alloc += max_p

    # trim excess in reverse to technical minimums
    excess = total_alloc - net_demand
    if excess > 0:
        for alloc in reversed(allocation):
            reducible = alloc["allocated_gen"] - alloc["min_gen"]
            if reducible <= 0:
                continue
            red = reducible if reducible < excess else excess
            alloc["allocated_gen"] -= red
            excess -= red
            if excess <= 0:
                break

    plant_dict = {p["Code"]: p for p in plants}
    final_list, total_cost = [], 0.0

    for alloc in allocation:
        base = _map_and_calculate(alloc, plant_dict)
        plf_pct = base["plf"] * 100.0

        # find matching band
        SHR = Aux = 0.0
        for row in backdown_table:
            if row["lower"] <= plf_pct <= row["upper"]:
                SHR, Aux = row["SHR"], row["Aux_Consumption"]
                break

        var_cost = base["Variable_Cost"]
        max_gen = base["max_power"]
        gen = base["generated_energy"]

        denom = (1.0 - Aux / 100.0)
        rate = 0.0 if denom <= 0 else var_cost * ((1.0 + SHR / 100.0) / denom)
        backdown_rate = round(rate, 2)
        backdown_qty = max_gen - gen
        backdown_cost = round(backdown_rate * backdown_qty, 2)

        base["backdown_rate"] = backdown_rate
        base["backdown_cost"] = backdown_cost
        base["backdown_unit"] = round(backdown_qty if backdown_rate else 0.0, 3)

        final_list.append(base)
        total_cost += base["net_cost"]

    final_list.sort(key=lambda x: x["Variable_Cost"])
    return {"other_plant_data": final_list, "total_cost": round(total_cost, 2)}


def _parse_ts_wi(ts: Union[str, datetime]) -> datetime:
    """Accepts datetime or WI-style string 'YYYY-DD-MM[ T]HH:MM[:SS]'."""
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        for fmt in ("%Y-%d-%m %H:%M:%S",
                    "%Y-%d-%m %H:%M",
                    "%Y-%d-%mT%H:%M:%S",
                    "%Y-%d-%mT%H:%M"):
            try:
                return datetime.strptime(ts.strip(), fmt)
            except ValueError:
                pass
    raise ValueError(f"Unrecognized timestamp format: {ts!r}. Expected 'YYYY-DD-MM HH:MM[:SS]'.")


def _get_must_run(banked_kwh: float, ts_dt: Union[str, datetime]) -> Dict[str, Any]:
    if banked_kwh <= 0:
        return {"plant_data": [], "generated_energy_all": 0.0, "total_cost": 0.0}

    # Normalize ts_dt (supports WI format 'YYYY-DD-MM HH:MM[:SS]')
    ts_dt = _parse_ts_wi(ts_dt)

    plants = _load_must_run_plants()
    codes = [p["Code"] for p in plants]

    cutoff_date = datetime(2024, 4, 1, 0, 0, 0)

    preds = {}
    for doc in MustRunPred.find(
            {"TimeStamp": ts_dt, "Plant_Name": {"$in": codes}},
            {"Plant_Name": 1, "Pred": 1, "Actual": 1, "_id": 0}
    ):
        actual_val = float(doc.get("Actual", 0.0) or 0.0)
        pred_val = float(doc.get("Pred", 0.0) or 0.0)

        if ts_dt < cutoff_date:
            preds[doc["Plant_Name"]] = actual_val
        else:
            preds[doc["Plant_Name"]] = actual_val if actual_val > 0 else pred_val

    data, gen_all, cost_all = [], 0.0, 0.0
    for p in plants:
        code = p["Code"]
        mw_val = preds.get(code, 0.0)
        gen_kwh = round(mw_val * 1000.0 * 0.25, 3)  # 15-min block

        var_cost = float(p["Variable_Cost"])
        net_cost = round(gen_kwh * var_cost, 2)

        # Treat Max_Power as per-block max energy (consistent with your other function's max_gen)
        max_energy = float(p.get("Max_Power", 0.0))

        gen_all += gen_kwh
        cost_all += net_cost

        data.append({
            "plant_name": p["name"],
            "plant_code": code,
            "Rated_Capacity": p["Rated_Capacity"],
            "PAF": p["PAF"],
            "PLF": p["PLF"],
            "Type": p["Type"],
            "Aux_Consumption": p["Aux_Consumption"],
            "Variable_Cost": var_cost,
            "generated_energy": gen_kwh,
            "max_power": max_energy,  # kept your field name
            "min_power": p["Min_Power"],
            "energy_not_taken": round(max_energy - gen_kwh, 3),  # ← NEW
            "net_cost": net_cost,
        })

    return {
        "plant_data": data,
        "generated_energy_all": round(gen_all, 3),
        "total_cost": round(cost_all, 2)
    }


def _get_exchange(ts_dt: datetime, cap_price: float) -> Dict[str, float]:
    doc = IEX_Gen.find_one({"TimeStamp": ts_dt}, {"Pred_Price": 1, "Qty_Pred": 1, "_id": 0})
    if not doc:
        return {"Pred_Price": 0.0, "Qty_Pred": 0.0}
    price = float(doc.get("Pred_Price", 0.0))
    qty = float(doc.get("Qty_Pred", 0.0)) * 1000.0 * 0.25
    return {
        "Pred_Price": 0.0 if price > float(cap_price) else price,
        "Qty_Pred": round(qty, 3),
    }


def _get_other_run(net2_kwh: float, ts_dt: datetime) -> Dict[str, Any]:
    if net2_kwh <= 0:
        return {"other_plant_data": [], "total_cost": 0.0}

    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    month_col = month_map[ts_dt.month]

    plants = _load_other_plants(month_col)
    backdown_table = _load_backdown_table()
    return _allocate_generation(plants, float(net2_kwh), backdown_table)


# ───────────────────── Main endpoint ─────────────────────
@router.get("/", response_class=JSONResponse, description="MOD Pricing")
def get_MOD(
        request: Request,  # ✅ Add this to access headers
        start_date: str = Query(..., description="Accepts 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', or 'YYYY-MM-DD HH:MM:SS'"),
        price_cap: float = Query(0, description="IEX price cap"),
):
    try:
        ts_dt = du.parse_start_timestamp(start_date)
        ts_str = ts_dt.strftime("%Y-%m-%d %H:%M:%S")

        demand_doc = Demand.find_one({"TimeStamp": ts_dt}, {"Demand(Actual)": 1, "Demand(Pred)": 1, "_id": 0})
        if not demand_doc:
            raise HTTPException(status_code=404, detail="No demand data found for the given date")

        d_actual = to_float(demand_doc.get("Demand(Actual)", 0.0))
        d_pred = to_float(demand_doc.get("Demand(Pred)", 0.0))

        bank_doc = Banking.find_one({"TimeStamp": ts_dt}, {"Banking_Unit": 1, "_id": 0})
        banking_unit = round(to_float(bank_doc.get("Banking_Unit", 0.0)) if bank_doc else 0.0, 3)

        actual_kwh = round(d_actual * 1000.0 * 0.25, 3)
        pred_kwh = round(d_pred * 1000.0 * 0.25, 3)
        base_kwh = pred_kwh if actual_kwh == 0 else actual_kwh
        banked_kwh = max(base_kwh - banking_unit, 0.0)

        must = _get_must_run(banked_kwh, ts_dt)
        iex = _get_exchange(ts_dt, price_cap)
        iex_cost = iex["Pred_Price"] * iex["Qty_Pred"] if iex["Pred_Price"] else 0.0
        iex_gen = iex["Qty_Pred"] if iex["Pred_Price"] else 0.0

        net1 = max(banked_kwh - must["generated_energy_all"], 0.0)
        net2 = max(net1 - iex_gen, 0.0)

        other = _get_other_run(net2, ts_dt)
        rem_plants = other["other_plant_data"]
        rem_cost = other["total_cost"]
        rem_gen = round(sum(p["generated_energy"] for p in rem_plants), 3)

        if banking_unit == 0:
            for p in rem_plants:
                p["backdown_cost"] = 0.0
                p["backdown_unit"] = 0.0
            total_backdown = 0.0
            backdown_unit = 0.0
            min_back_cost = 0.0
        else:
            total_backdown = round(sum(p["backdown_cost"] for p in rem_plants), 2)
            backdown_unit = round(sum(p.get("backdown_unit", 0.0) for p in rem_plants), 3)
            min_back_cost = round(
                min((p["backdown_rate"] for p in rem_plants if p.get("backdown_unit", 0.0) > 0), default=0.0), 2
            )

        iex_price = iex["Pred_Price"] if iex["Qty_Pred"] > 0 else 0.0
        last_price = iex_price
        if rem_plants:
            last_price = max(round(rem_plants[-1]["Variable_Cost"], 2), iex_price)

        denom = banked_kwh if banked_kwh else 0.0
        cost_per_block = round((must["total_cost"] + iex_cost + rem_cost) / denom, 2) if denom else 0.0

        result = OrderedDict({
            "TimeStamp": ts_str,
            "Demand(Actual)": actual_kwh,
            "Demand(Pred)": pred_kwh,
            "Banking_Unit": banking_unit,
            "Demand_Banked": round(banked_kwh, 3),

            "Backdown_Cost_Min": min_back_cost,

            "Must_Run": must["plant_data"],
            "Must_Run_Total_Gen": must["generated_energy_all"],
            "Must_Run_Total_Cost": must["total_cost"],

            "IEX_Data": iex,
            "IEX_Gen": round(iex_gen, 3),
            "IEX_Cost": round(iex_cost, 2),

            "Remaining_Plants": rem_plants,
            "Remaining_Plants_Total_Gen": rem_gen,
            "Remaining_Plants_Total_Cost": rem_cost,

            "Last_Price": round(last_price, 2),
            "Cost_Per_Block": cost_per_block,

            "Backdown_Cost": round(total_backdown, 2) if banking_unit > 0 else 0.0,
            "Backdown_Unit": backdown_unit if banking_unit > 0 else 0.0,
        })

        # ✅ Add metadata fields for logging
        uploaded_by = request.headers.get("X-User-Email", "unknown")
        uploaded_date = datetime.utcnow() + timedelta(hours=5, minutes=30)

        # Atomic upsert (safer than delete + insert)
        DemandOutput.replace_one(
            {"TimeStamp": ts_dt},
            {**result,
             "TimeStamp": ts_dt,
             "uploaded_by": uploaded_by,
             "uploaded_date": uploaded_date,
             },
            upsert=True
        )

        return JSONResponse(content=result, status_code=200)

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", response_class=JSONResponse, description="Block-wise summary from Demand_Output")
def get_summary(
        start_date: str = Query(..., description="Accepts 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', or 'YYYY-MM-DD HH:MM:SS'"),
        end_date: str = Query(..., description="Accepts 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', or 'YYYY-MM-DD HH:MM:SS'")
):
    try:
        start_dt = du.parse_start_timestamp(start_date)
        end_dt = du.parse_start_timestamp(end_date)

        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="start_date must be <= end_date")

        projection = {
            "_id": 0,
            "TimeStamp": 1,
            "Demand_Banked": 1,
            "Demand(Actual)": 1,
            "Must_Run_Total_Gen": 1,
            "Must_Run_Total_Cost": 1,
            "IEX_Gen": 1,
            "IEX_Cost": 1,
            "Remaining_Plants_Total_Gen": 1,
            "Remaining_Plants_Total_Cost": 1,
            "Last_Price": 1,
            "Backdown_Cost": 1,
            "Backdown_Unit": 1,
            "Banking_Unit": 1,
        }

        cursor = DemandOutput.find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            projection
        ).sort("TimeStamp", 1)

        def _coerce_numeric(v):
            try:
                return to_float(v)
            except Exception:
                return v

        results: List[Dict[str, Any]] = []
        for doc in cursor:
            clean: Dict[str, Any] = {}

            # 1) Handle TimeStamp first (no numeric coercion)
            ts = doc.get("TimeStamp")
            if isinstance(ts, datetime):
                clean["TimeStamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # keep as-is if already string/number (but ideally it's a datetime)
                clean["TimeStamp"] = ts

            # 2) Coerce other numeric fields
            for k, v in doc.items():
                if k == "TimeStamp":
                    continue
                try:
                    clean[k] = to_float(v)
                except Exception:
                    clean[k] = v

            results.append(clean)

        return JSONResponse(content={"summary": results}, status_code=200)

    except HTTPException:
        raise
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

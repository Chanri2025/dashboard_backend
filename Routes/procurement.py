from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from collections import OrderedDict
import mysql.connector
from pymongo import MongoClient
from bson import Decimal128
import os
from dotenv import load_dotenv
from fastapi.responses import JSONResponse
from utils.date_utils import parse_timestamp_any, parse_start_timestamp

router = APIRouter()

load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["powercasting"]
collection = db["Demand_Output"]


def to_float(val):
    if isinstance(val, Decimal128):
        return float(val.to_decimal())
    return float(val or 0)


@router.get("/range")
def get_demand_output_range(start: str = Query(...), end: str = Query(...)):
    try:
        start_dt = parse_timestamp_any(start)
        end_dt = parse_timestamp_any(end)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp format: {e}")

    cursor = collection.find({"TimeStamp": {"$gte": start_dt, "$lte": end_dt}}, {"_id": 0}).sort("TimeStamp", 1)
    docs = list(cursor)

    rows = []
    for doc in docs:
        ts_str = doc["TimeStamp"].strftime("%a, %d %b %Y %H:%M:%S GMT")
        rows.append({
            "timestamp": ts_str,
            "cost_per_block": doc.get("Cost_Per_Block", 0),
            "last_price": doc.get("Last_Price", 0)
        })

    total_cost_per_block = sum(r["cost_per_block"] for r in rows)
    average_cost_per_block = total_cost_per_block / len(rows) if rows else None
    total_mod = sum(r["last_price"] for r in rows)
    average_mod = total_mod / len(rows) if rows else None

    return JSONResponse({
        "data": rows,
        "summary": {
            "total_cost_per_block": total_cost_per_block,
            "average_cost_per_block": round(average_cost_per_block, 2) if average_cost_per_block is not None else None,
            "total_mod": total_mod,
            "average_mod": round(average_mod, 2) if average_mod is not None else None
        }
    })


@router.get("/")
def get_procurement_summary(start_date: str = Query(...), price_cap: float = Query(0.0)):
    try:
        start_date = start_date[:19]
        start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")

        demand_doc = db["Demand"].find_one({"TimeStamp": start_dt})
        if not demand_doc:
            raise HTTPException(status_code=404, detail="No demand data found for the given date")

        d_actual = to_float(demand_doc.get("Demand(Actual)", 0))
        d_pred = to_float(demand_doc.get("Demand(Pred)", 0))

        bank_doc = db["Banking_Data"].find_one({"TimeStamp": start_dt})
        banking_unit = round(to_float(bank_doc.get("Banking_Unit", 0)) if bank_doc else 0.0, 3)

        actual_kwh = round(d_actual * 1000 * 0.25, 3)
        pred_kwh = round(d_pred * 1000 * 0.25, 3)

        base_kwh = pred_kwh if actual_kwh == 0 else actual_kwh
        banked_kwh = base_kwh - banking_unit

        output_doc = db["Demand_Output"].find_one({"TimeStamp": start_dt})
        if not output_doc:
            raise HTTPException(status_code=404, detail="No output data found for the given date")

        must_run = output_doc.get("Must_Run", [])
        must_gen = output_doc.get("Must_Run_Total_Gen", 0.0)
        must_cost = output_doc.get("Must_Run_Total_Cost", 0.0)

        iex_data = output_doc.get("IEX_Data", {})
        iex_gen = output_doc.get("IEX_Gen", 0.0)
        iex_cost = output_doc.get("IEX_Cost", 0.0)

        rem_plants = output_doc.get("Remaining_Plants", [])
        rem_gen = output_doc.get("Remaining_Plants_Total_Gen", 0.0)
        rem_cost = output_doc.get("Remaining_Plants_Total_Cost", 0.0)

        total_backdown = sum(p.get("backdown_cost", 0) for p in rem_plants) if banking_unit > 0 else 0.0
        backdown_unit = sum(p.get("backdown_unit", 0) for p in rem_plants) if banking_unit > 0 else 0.0
        min_backdown_cost = min((p.get("backdown_rate", 0) for p in rem_plants if p.get("backdown_unit", 0) > 0),
                                default=0.0)

        iex_price = iex_data.get("Pred_Price", 0.0) if iex_data.get("Qty_Pred", 0) > 0 else 0.0
        last_price = max(round(rem_plants[-1]["Variable_Cost"], 2), iex_price) if rem_plants else iex_price

        cost_per_block = round((must_cost + iex_cost + rem_cost) / banked_kwh, 2) if banked_kwh else 0.0

        result = OrderedDict({
            "TimeStamp": start_date,
            "Demand(Actual)": actual_kwh,
            "Demand(Pred)": pred_kwh,
            "Banking_Unit": banking_unit,
            "Demand_Banked": banked_kwh,
            "Backdown_Cost_Min": round(min_backdown_cost, 2) if banking_unit > 0 else 0.0,
            "Must_Run": must_run,
            "Must_Run_Total_Gen": must_gen,
            "Must_Run_Total_Cost": must_cost,
            "IEX_Data": iex_data,
            "IEX_Gen": round(iex_gen, 3),
            "IEX_Cost": round(iex_cost, 2),
            "Remaining_Plants": rem_plants,
            "Remaining_Plants_Total_Gen": round(rem_gen, 3),
            "Remaining_Plants_Total_Cost": round(rem_cost, 2),
            "Last_Price": round(last_price, 2),
            "Cost_Per_Block": round(cost_per_block, 2),
            "Backdown_Cost": round(total_backdown, 2) if banking_unit > 0 else 0.0,
            "Backdown_Unit": round(backdown_unit, 2) if banking_unit > 0 else 0.0
        })

        return JSONResponse(content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/range")
def get_demand_output_range(start: str = Query(...), end: str = Query(...)):
    try:
        start_dt = parse_timestamp_any(start)
        end_dt = parse_timestamp_any(end)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp format: {e}")

    cursor = collection.find({"TimeStamp": {"$gte": start_dt, "$lte": end_dt}}, {"_id": 0}).sort("TimeStamp", 1)
    docs = list(cursor)

    rows = []
    for doc in docs:
        ts_str = doc["TimeStamp"].strftime("%a, %d %b %Y %H:%M:%S GMT")
        rows.append({
            "timestamp": ts_str,
            "cost_per_block": doc.get("Cost_Per_Block", 0),
            "last_price": doc.get("Last_Price", 0)
        })

    total_cost_per_block = sum(r["cost_per_block"] for r in rows)
    average_cost_per_block = total_cost_per_block / len(rows) if rows else None
    total_mod = sum(r["last_price"] for r in rows)
    average_mod = total_mod / len(rows) if rows else None

    return JSONResponse({
        "data": rows,
        "summary": {
            "total_cost_per_block": total_cost_per_block,
            "average_cost_per_block": round(average_cost_per_block, 2) if average_cost_per_block is not None else None,
            "total_mod": total_mod,
            "average_mod": round(average_mod, 2) if average_mod is not None else None
        }
    })

# main.py
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query, Request
from pymongo import MongoClient, ASCENDING
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from typing import Any, Dict, List

from utils.mongo_index import ensure_index
from utils.date_utils import parse_start_timestamp, parse_end_timestamp
from utils.mongo_helpers import convert_decimal128

from middlewares.transaction_logger_middleware import TransactionLoggerMiddleware

# ── Routers
from routes.auth import router as auth_router
from routes.availability import router as availability_router
from routes.backdown import router as backdown_router
from routes.consolidated import router as consolidated_router
from routes.consolidated_2 import router as consolidated_2_router
from routes.consumer import router as consumer_router
from routes.demand import router as demand_router
from routes.dtr import router as dtr_router
from routes.feeder import router as feeder_router
from routes.iex import router as iex_router
from routes.plant import router as plant_router
from routes.power_theft import router as power_theft_router
from routes.procurement import router as procurement_router
from routes.region import router as region_router
from routes.substation import router as substation_router
from routes.consumption import router as consumption_router
from routes.billing import router as billing_router
from routes.complaints import router as complaints_router

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError("MONGO_URI is not set")

    # Async client (for Motor-using routes)
    am_client = AsyncIOMotorClient(mongo_uri)
    app.state.mongo_db = am_client["powercasting"]

    # Sync client (for PyMongo/indexes and sync routes)
    sm_client = MongoClient(mongo_uri)
    app.state.mongo_sync = sm_client
    mdb = sm_client["powercasting"]

    # 👉 also open the DB where your consolidated docs are stored
    mdb_new = sm_client["power_casting_new"]  # <— added

    # 👉 expose the DB handle that your routes expect
    app.state.mongo_sync_db = mdb

    # Optional: verify connection early (fail fast)
    try:
        sm_client.admin.command("ping")
    except Exception as e:
        am_client.close()
        sm_client.close()
        raise RuntimeError(f"MongoDB ping failed: {e}") from e

    # Build indexes once, idempotently
    drop_mismatch = os.getenv("ALLOW_INDEX_DROP", "false").lower() == "true"

    # Existing indexes (powercasting)
    ensure_index(mdb["Demand"], [("TimeStamp", ASCENDING)], name="ts", unique=False, drop_if_mismatch=drop_mismatch)
    ensure_index(mdb["Banking_Data"], [("TimeStamp", ASCENDING)], name="ts", unique=False,
                 drop_if_mismatch=drop_mismatch)
    ensure_index(mdb["IEX_Generation"], [("TimeStamp", ASCENDING)], name="ts", unique=False,
                 drop_if_mismatch=drop_mismatch)
    ensure_index(mdb["mustrunplantconsumption"], [("TimeStamp", ASCENDING), ("Plant_Name", ASCENDING)],
                 name="ts_plant", unique=False, drop_if_mismatch=drop_mismatch)
    ensure_index(mdb["Demand_Output"], [("TimeStamp", ASCENDING)], name="ts", unique=True,
                 drop_if_mismatch=drop_mismatch)

    # ✅ NEW: index for consolidated collection in power_casting_new
    ensure_index(
        mdb_new["Banking-Adjust-consolidated"],
        [("Timestamp", ASCENDING)],
        name="timestamp_unique",
        unique=True,
        drop_if_mismatch=drop_mismatch,
    )

    try:
        yield
    finally:
        am_client.close()
        sm_client.close()


app = FastAPI(title="Power Casting API", debug=True, lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=os.getenv("CORS_CREDENTIALS", "false").lower() == "true",
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(TransactionLoggerMiddleware)

# Register Routers
app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(availability_router, prefix="/availability", tags=["Availability"])
app.include_router(backdown_router, prefix="/backdown", tags=["Backdown"])
app.include_router(consolidated_router, prefix="/consolidated-part", tags=["Consolidated"])
app.include_router(consolidated_2_router, prefix="/consolidated-part", tags=["Consolidated"])
app.include_router(consumer_router, prefix="/consumer", tags=["Consumer"])
app.include_router(consumption_router, prefix="/consumer", tags=["Consumer"])
app.include_router(demand_router, prefix="/demand", tags=["Demand"])
app.include_router(dtr_router, prefix="/dtr", tags=["DTR"])
app.include_router(feeder_router, prefix="/feeder", tags=["Feeder"])
app.include_router(iex_router, prefix="/iex", tags=["IEX"])
app.include_router(plant_router, prefix="/plant", tags=["Plant"])
app.include_router(power_theft_router, prefix="/power-theft", tags=["Power - Theft"])
app.include_router(procurement_router, prefix="/procurement", tags=["Procurement"])
app.include_router(region_router, prefix="/region", tags=["Region"])
app.include_router(substation_router, prefix="/substation", tags=["Sub - Station"])
app.include_router(complaints_router, prefix="/complaints", tags=["Complaints"])
app.include_router(billing_router, prefix="/billing", tags=["Billing"])


# ── Dashboard Route ────────────────────────────────────────────────
@app.get("/dashboard")
async def get_dashboard(
        request: Request,
        start_date: str = Query(..., description="YYYY-MM-DD[ HH:MM[:SS]]"),
        end_date: str = Query(..., description="YYYY-MM-DD[ HH:MM[:SS]]"),
):
    # Validate/parse timestamps using your helpers
    try:
        start_dt = parse_start_timestamp(start_date)
        end_dt = parse_end_timestamp(end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    # PyMongo DB handle from app state
    db = request.app.state.mongo_sync_db
    demand_coll = db["Demand"]
    iex_coll = db["IEX_Price"]
    procurement_coll = db["Demand_Output"]

    # ── Demand ─────────────────────────────────────────────────────
    demand_rows: List[Dict[str, Any]] = []
    for raw in demand_coll.find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0},
    ):
        doc = convert_decimal128(raw)
        ts = doc.get("TimeStamp")
        if isinstance(ts, datetime):
            doc["TimeStamp"] = ts.strftime("%a, %d %b %Y %H:%M:%S GMT")
        demand_rows.append(doc)

    # ── IEX ────────────────────────────────────────────────────────
    iex_rows: List[Dict[str, Any]] = []
    for raw in iex_coll.find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0},
    ):
        doc = convert_decimal128(raw)
        ts = doc.get("TimeStamp")
        if isinstance(ts, datetime):
            doc["TimeStamp"] = ts.strftime("%a, %d %b %Y %H:%M:%S GMT")
        iex_rows.append(doc)

    # ── Procurement ───────────────────────────────────────────────
    procurement_rows: List[Dict[str, Any]] = []
    for raw in procurement_coll.find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0},
    ):
        doc = convert_decimal128(raw)

        ts_orig = doc.get("TimeStamp")
        if isinstance(ts_orig, datetime):
            ts_str = ts_orig.strftime("%a, %d %b %Y %H:%M:%S GMT")
        else:
            ts_str = ts_orig

        rec = {
            "backdown_total_cost": doc.get("Backdown_Cost", 0),
            "backdown_cost_min": doc.get("Backdown_Cost_Min", 0),
            "backdown_unit": doc.get("Backdown_Unit", 0),
            "banking_unit": doc.get("Banking_Unit", 0),
            "cost_per_block": doc.get("Cost_Per_Block", 0),
            "demand_actual": doc.get("Demand(Actual)", 0),
            "demand_banked": doc.get("Demand_Banked", 0),
            "demand_pred": doc.get("Demand(Pred)", 0),
            "iex_cost": doc.get("IEX_Cost", 0),
            "iex_data": doc.get("IEX_Data", {}) or {},
            "iex_gen": doc.get("IEX_Gen", 0),
            "last_price": doc.get("Last_Price", 0),
            "must_run": doc.get("Must_Run", []),
            "must_run_total_cost": doc.get("Must_Run_Total_Cost", 0),
            "must_run_total_gen": doc.get("Must_Run_Total_Gen", 0),
            "remaining_plants": doc.get("Remaining_Plants", []),
            "remaining_plants_total_cost": doc.get("Remaining_Plants_Total_Cost", 0),
            "remaining_plants_total_gen": doc.get("Remaining_Plants_Total_Gen", 0),
            "timestamp": ts_str,
        }

        nested = rec["iex_data"]
        nts = nested.get("TimeStamp")
        if isinstance(nts, datetime):
            nested["TimeStamp"] = nts.strftime("%a, %d %b %Y %H:%M:%S GMT")
        rec["iex_data"] = nested

        procurement_rows.append(rec)

    return {
        "demand": demand_rows,
        "iex": iex_rows,
        "procurement": procurement_rows,
    }


@app.get("/")
async def root():
    return {"message": "GUVNL is running!"}


if __name__ == "__main__":
    import uvicorn

    # reload and workers>1 are incompatible; pick ONE
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)

from bson.decimal128 import Decimal128
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient

from Routes.AdjustmentRoutes import adjustingAPI
from Routes.consolidatedRoutes import consolidatedAPI
from Routes.demandRoutes import demandApi
from Routes.iexRoutes import iexApi
from Routes.procurementRoutes import procurementAPI
from Routes.plantRoutes import plantAPI
from Routes.BankingRoutes import bankingAPI
from Routes.availibilityfactorRoutes import availabilityAPI
from Routes.regionRoutes import regionApi
from Routes.feederRoutes import feederApi
from Routes.powerTheftRoutes import powerTheftApi
from Routes.divisionRoutes import divisionApi
from Routes.substationRoutes import substationApi
from Routes.consumerRoutes import consumerApi
from Routes.dtrRoutes import dtrApi
from Routes.BackdownRoutes import backDownApi
import mysql.connector
import json
from dotenv import load_dotenv
import os
from datetime import datetime

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1]  # Using guvnl_consumers for main app
}
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Enable CORS for all routes and origins

# Register the Blueprint
app.register_blueprint(procurementAPI, url_prefix='/procurement')  # Registering the Procurement API
app.register_blueprint(plantAPI, url_prefix='/plant')  # Registering the Plant API
app.register_blueprint(demandApi, url_prefix='/demand')  # Registering the Demand API
app.register_blueprint(bankingAPI, url_prefix='/banking')  # Registering the Banking API
app.register_blueprint(adjustingAPI, url_prefix='/adjusting') # Registering the Adjusting API
app.register_blueprint(consolidatedAPI, url_prefix='/consolidated-part') # Registering the Consolidated API
app.register_blueprint(iexApi, url_prefix='/iex')  # Registering the IEX API
app.register_blueprint(availabilityAPI, url_prefix='/availability')  # Registering the Plant availability factor API
app.register_blueprint(backDownApi, url_prefix='/backdown')  # Registering the Backdown API
app.register_blueprint(regionApi, url_prefix='/region')  # Registering the Region API
app.register_blueprint(divisionApi, url_prefix='/division')  # Registering the Division API
app.register_blueprint(substationApi, url_prefix='/substation')  # Registering the Substation API
app.register_blueprint(feederApi, url_prefix='/feeder')  # Registering the Feeder API
app.register_blueprint(dtrApi, url_prefix='/dtr')  # Registering the DTR API
app.register_blueprint(consumerApi, url_prefix='/consumer')  # Registering the Consumer API
app.register_blueprint(powerTheftApi, url_prefix='/power-theft')  # Registering the Power Theft API

# ——— Mongo config ———
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = 'powercasting'

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
demand_coll = db['Demand']
iex_coll = db['IEX_Price']
procurement_coll = db['Demand_Output']


def parse_iso(ts_str: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"time data {ts_str!r} does not match any supported format")


def convert_decimal128(obj):
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    if isinstance(obj, dict):
        return {k: convert_decimal128(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal128(v) for v in obj]
    return obj


@app.route('/dashboard', methods=['GET'])
def get_data_with_sum():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Start date and end date parameters are required"}), 400

    try:
        start_dt = parse_iso(start_date)
        end_dt = parse_iso(end_date)
    except ValueError as e:
        return jsonify({"error": f"Invalid date format: {e}"}), 400

    # ── Demand ─────────────────────────────────────────────────────
    demand_rows = []
    for raw in demand_coll.find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0}
    ):
        doc = convert_decimal128(raw)
        ts = doc.get("TimeStamp")
        if isinstance(ts, datetime):
            doc["TimeStamp"] = ts.strftime("%a, %d %b %Y %H:%M:%S GMT")
        demand_rows.append(doc)

    # ── IEX ────────────────────────────────────────────────────────
    iex_rows = []
    for raw in iex_coll.find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0}
    ):
        doc = convert_decimal128(raw)
        ts = doc.get("TimeStamp")
        if isinstance(ts, datetime):
            doc["TimeStamp"] = ts.strftime("%a, %d %b %Y %H:%M:%S GMT")
        iex_rows.append(doc)

    # ── Procurement ───────────────────────────────────────────────
    procurement_rows = []
    for raw in procurement_coll.find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0}
    ):
        doc = convert_decimal128(raw)

        # format the main timestamp
        ts_orig = doc.get("TimeStamp")
        if isinstance(ts_orig, datetime):
            ts_str = ts_orig.strftime("%a, %d %b %Y %H:%M:%S GMT")
        else:
            ts_str = ts_orig  # assume it's already a string

        # remap into snake_case
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
            "iex_data": doc.get("IEX_Data", {}),
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

        # format nested IEX_Data.TimeStamp if present
        nested = rec["iex_data"]
        nts = nested.get("TimeStamp")
        if isinstance(nts, datetime):
            nested["TimeStamp"] = nts.strftime("%a, %d %b %Y %H:%M:%S GMT")

        procurement_rows.append(rec)

    return jsonify({
        "demand": demand_rows,
        "iex": iex_rows,
        "procurement": procurement_rows
    }), 200


@app.route('/')
def hello_world():
    return 'GUVNL is running!'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)  # Run the app on all available IP addresses

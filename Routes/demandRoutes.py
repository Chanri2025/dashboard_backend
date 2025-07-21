from flask import Blueprint, jsonify, request
from datetime import datetime
import mysql.connector
from pymongo import MongoClient
from bson.decimal128 import Decimal128
import os
from dotenv import load_dotenv

# Create a Blueprint
demandApi = Blueprint('demand', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1],
}

mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
db = client["powercasting"]


def to_float(val):
    """Convert Decimal128 (or anything numeric) into a Python float."""
    if isinstance(val, Decimal128):
        return float(val.to_decimal())
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def parse_dt(s: str) -> datetime:
    """
    Try parsing 'YYYY-MM-DD HH:MM:SS' then 'YYYY-MM-DD HH:MM'.
    If the latter, appends ':00' seconds.
    """
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"time data {s!r} does not match formats "
                     "'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD HH:MM'")


def _convert_decimal128(obj):
    """
    Recursively convert any Decimal128 in dicts/lists into Python floats.
    """
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    if isinstance(obj, dict):
        return {k: _convert_decimal128(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_decimal128(v) for v in obj]
    return obj


@demandApi.route('/dashboard', methods=['GET'])
def get_dashboard_data():
    try:
        # 1) Parse optional start/end
        start = request.args.get('start')
        end = request.args.get('end')
        match = {}
        if start:
            try:
                match.setdefault("TimeStamp", {})["$gte"] = parse_dt(start)
            except ValueError as ve:
                return jsonify({"error": str(ve)}), 400
        if end:
            try:
                match.setdefault("TimeStamp", {})["$lte"] = parse_dt(end)
            except ValueError as ve:
                return jsonify({"error": str(ve)}), 400

        # 2) MySQL: plant count
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT COUNT(TYPE) AS count FROM `plant_details`;")
        plant_count_row = cursor.fetchone()
        cursor.close()
        conn.close()
        plant_count = plant_count_row["count"] if plant_count_row else 0

        # 3) MongoDB: sum Demand
        demand_pipeline = []
        if match:
            demand_pipeline.append({"$match": match})
        demand_pipeline.append({
            "$group": {
                "_id": None,
                "total_actual": {"$sum": "$Demand(Actual)"},
                "total_predicted": {"$sum": "$Demand(Pred)"}
            }
        })
        demand_res = list(db["Demand"].aggregate(demand_pipeline))
        total_actual = to_float(demand_res[0]["total_actual"]) if demand_res else 0.0
        total_predicted = to_float(demand_res[0]["total_predicted"]) if demand_res else 0.0

        # 4) MongoDB: avg Cost_Per_Block in Demand_Output
        output_pipeline = []
        if match:
            output_pipeline.append({"$match": match})
        output_pipeline.append({
            "$group": {"_id": None, "avg_price": {"$avg": "$Cost_Per_Block"}}
        })
        avg_res = list(db["Demand_Output"].aggregate(output_pipeline))
        average_price = to_float(avg_res[0]["avg_price"]) if avg_res else 0.0

        # 5) Build and return
        return jsonify({
            "plant_count": plant_count,
            "demand_actual": round(total_actual, 3),
            "demand_predicted": round(total_predicted, 3),
            "avg_price": round(average_price, 2)
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# NEW ENDPOINT FOR DATE RANGE DATA
@demandApi.route('/range', methods=['GET'])
def get_demand_data():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    if not start_date or not end_date:
        return jsonify({"error": "Both 'start_date' and 'end_date' parameters are required"}), 400

    # 1) parse the inputs
    try:
        start_dt = parse_dt(start_date)
        end_dt = parse_dt(end_date)
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    try:
        # 2) query MongoDB
        cursor = db["Demand"] \
            .find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0}
        ) \
            .sort("TimeStamp", 1)

        raw_docs = list(cursor)

        # 3) clean & format
        clean_docs = []
        for doc in raw_docs:
            doc = _convert_decimal128(doc)
            ts = doc.get("TimeStamp")
            if isinstance(ts, datetime):
                doc["TimeStamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")
            clean_docs.append(doc)

        # 4) return
        return jsonify({"demand": clean_docs}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

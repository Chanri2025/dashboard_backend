# iexRoutes.py
from datetime import datetime
from flask import Blueprint, jsonify, request
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.decimal128 import Decimal128

# Create a Blueprint
iexApi = Blueprint('iex', __name__)

# load .env
load_dotenv()

# MongoDB setup
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
db = client["powercasting"]


def _convert_decimal128(obj):
    """
    Recursively convert any Decimal128 in a dict or list into float.
    """
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    if isinstance(obj, dict):
        return {k: _convert_decimal128(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_decimal128(v) for v in obj]
    return obj


def to_float(val):
    """
    Convert a Decimal128 (or other numeric) into a Python float.
    """
    if isinstance(val, Decimal128):
        return float(val.to_decimal())
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


@iexApi.route('/all', methods=['GET'])
def get_price_data():
    try:
        # Query all documents (excluding the _id)
        raw_docs = list(db["IEX_Price"].find({}, {'_id': 0}))

        # First, convert any Decimal128 → float, recursively
        clean_docs = [_convert_decimal128(doc) for doc in raw_docs]

        # Then convert datetime timestamps to strings
        for item in clean_docs:
            ts = item.get('TimeStamp')
            if isinstance(ts, datetime):
                item['TimeStamp'] = ts.strftime("%Y-%m-%d %H:%M:%S")

        return jsonify(clean_docs), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexApi.route('/range', methods=['GET'])
def get_demand_range():
    start = request.args.get('start')  # e.g. "2021-04-01 00:00:00"
    end = request.args.get('end')  # e.g. "2021-04-02 00:00:00"

    if not start or not end:
        return jsonify({"error": "Both 'start' and 'end' query parameters are required"}), 400

    try:
        # Convert string dates to datetime objects for MongoDB query
        start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

        # Query MongoDB IEX_Generation collection
        iex_collection = db["IEX_Generation"]

        # Find documents between start and end dates
        query = {
            "TimeStamp": {
                "$gte": start_dt,
                "$lte": end_dt
            }
        }

        # Project only needed fields and sort by timestamp
        projection = {
            "_id": 0,
            "TimeStamp": 1,
            "Pred_Price": 1
        }

        # Execute query
        cursor = iex_collection.find(query, projection).sort("TimeStamp", 1)

        # Convert to list and format for response
        rows = []
        for doc in cursor:
            # Format datetime to string
            timestamp = doc["TimeStamp"].strftime("%Y-%m-%d %H:%M:%S") if isinstance(doc["TimeStamp"], datetime) else \
                doc["TimeStamp"]

            rows.append({
                "TimeStamp": timestamp,
                "predicted": doc.get("Pred_Price", 0)
            })

        # Calculate total and average
        total_predicted = sum(r['predicted'] for r in rows)
        average_predicted = total_predicted / len(rows) if rows else None

        return jsonify({
            "data": rows,
            "summary": {
                "total_predicted": total_predicted,
                "average_predicted": round(average_predicted, 2) if average_predicted else None
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexApi.route('/dashboard', methods=['GET'])
def get_dashboard():
    try:
        price_collection = db["IEX_Price"]

        # Parse optional start/end timestamps
        start = request.args.get('start')
        end = request.args.get('end')

        match_stage = {}
        if start:
            start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            match_stage.setdefault("TimeStamp", {})["$gte"] = start_dt
        if end:
            end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            match_stage.setdefault("TimeStamp", {})["$lte"] = end_dt

        # Build aggregation pipeline
        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})
        pipeline.append({
            "$group": {
                "_id": None,
                "Avg_Price": {"$avg": "$Actual"},
                "Avg_Pred_Price": {"$avg": "$Pred"}
            }
        })

        result = list(price_collection.aggregate(pipeline))

        if not result:
            # No data in range (or at all)
            return jsonify({"Avg_Price": 0, "Avg_Pred_Price": 0}), 200

        avg_doc = result[0]
        avg_price = to_float(avg_doc.get("Avg_Price"))
        avg_pred_price = to_float(avg_doc.get("Avg_Pred_Price"))

        rows = {
            "Avg_Price": round(avg_price, 2),
            "Avg_Pred_Price": round(avg_pred_price, 2)
        }

        return jsonify(rows), 200

    except ValueError as ve:
        # Bad timestamp format
        return jsonify({"error": f"Invalid timestamp: {ve}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexApi.route("/quantity", methods=["GET"])
def get_quantity_data():
    try:
        # Parse optional start/end filters
        start = request.args.get('start')
        end = request.args.get('end')
        match = {}
        if start:
            try:
                start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            except ValueError as ve:
                return jsonify({'error': f'Invalid start timestamp: {ve}'}), 400
            match.setdefault("TimeStamp", {})["$gte"] = start_dt
        if end:
            try:
                end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            except ValueError as ve:
                return jsonify({'error': f'Invalid end timestamp: {ve}'}), 400
            match.setdefault("TimeStamp", {})["$lte"] = end_dt

        # Fetch and filter
        raw_docs = list(db["IEX_Generation"].find(match if match else {}, {"_id": 0}))

        # 1) Convert Decimal128 → float
        clean_docs = [_convert_decimal128(doc) for doc in raw_docs]

        # 2) Format datetime to string
        for doc in clean_docs:
            ts = doc.get("TimeStamp")
            if isinstance(ts, datetime):
                doc["TimeStamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")

        return jsonify(clean_docs), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# iexRoutes.py
from datetime import datetime
from flask import Blueprint, jsonify, request
import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Create a Blueprint
iexApi = Blueprint('iex', __name__)

# load .env
load_dotenv()

# MongoDB setup
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
db = client["powercasting"]


@iexApi.route('/all', methods=['GET'])
def get_price_data():
    try:
        # Connect to MongoDB and get the IEX_Price collection
        price_collection = db["IEX_Price"]

        # Query all documents from the collection
        price_data = list(price_collection.find({}, {'_id': 0}))

        # If MongoDB returns dates as datetime objects, convert them to strings
        for item in price_data:
            if 'TimeStamp' in item and isinstance(item['TimeStamp'], datetime):
                item['TimeStamp'] = item['TimeStamp'].strftime("%Y-%m-%d %H:%M:%S")

        return jsonify(price_data), 200

    except Exception as e:
        # Handle any errors
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
        # Query MongoDB IEX_Price collection
        price_collection = db["IEX_Price"]

        # Use MongoDB aggregation to calculate averages
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "Avg_Price": {"$avg": "$Actual"},
                    "Avg_Pred_Price": {"$avg": "$Pred"}
                }
            }
        ]

        result = list(price_collection.aggregate(pipeline))

        if not result:
            return jsonify({"Avg_Price": 0, "Avg_Pred_Price": 0}), 200

        # Format the result
        rows = {
            "Avg_Price": round(float(result[0]["Avg_Price"]), 2),
            "Avg_Pred_Price": round(float(result[0]["Avg_Pred_Price"]), 2)
        }

        return jsonify(rows), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@iexApi.route("/quantity", methods=["GET"])
def get_quantity_data():
    try:
        # Query MongoDB IEX_Generation collection
        iex_collection = db["IEX_Generation"]

        # Get all documents, excluding _id field
        cursor = iex_collection.find({}, {"_id": 0})

        # Convert to list and format datetime fields
        rows = []
        for doc in cursor:
            # Convert datetime objects to strings
            if "TimeStamp" in doc and isinstance(doc["TimeStamp"], datetime):
                doc["TimeStamp"] = doc["TimeStamp"].strftime("%Y-%m-%d %H:%M:%S")

            rows.append(doc)

        return jsonify(rows), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

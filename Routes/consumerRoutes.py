import os
from datetime import datetime
import mysql.connector
from bson.decimal128 import Decimal128
from dotenv import load_dotenv
from flask import Blueprint, jsonify, request
from pymongo import MongoClient
from dateutil import parser

consumerApi = Blueprint('consumer', __name__)
load_dotenv()

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': 'guvnl_consumers',
}


@consumerApi.route('/consumption', methods=['GET'])
def get_lt_consumption_from_mongo():
    """
    Fetch LT consumption from MongoDB collection `powercasting.Consumer_Consumption`.
    Filters:
      • start_date, end_date (required, ISO 8601 or "YYYY-MM-DD HH:mm")
      • consumer_id          (optional)
    """
    # 1️⃣ Required time params
    start_str = request.args.get('start_date')
    end_str = request.args.get('end_date')
    if not start_str or not end_str:
        return jsonify({
            "error": "Both start_date and end_date query parameters are required (ISO format)."
        }), 400

    # 2️⃣ Parse datetime with fallback using dateutil.parser
    try:
        start = parser.parse(start_str)
        end = parser.parse(end_str)
    except (ValueError, TypeError):
        return jsonify({
            "error": "Invalid date format. Use ISO 8601, e.g. 2023-04-01T00:00:00"
        }), 400

    # 3️⃣ Optional consumer_id
    consumer_id = request.args.get('consumer_id').upper()

    try:
        # 4️⃣ Connect to MongoDB
        client = MongoClient(os.getenv("MONGO_URI"))
        coll = client["powercasting"]["Consumer_Consumption"]

        # 5️⃣ Build query
        query = {
            "Timestamp": {"$gte": start, "$lte": end}
        }
        if consumer_id:
            query["Consumer_id"] = consumer_id

        # 6️⃣ Retrieve documents
        cursor = coll.find(query, {"_id": False})
        results = []
        for doc in cursor:
            # Convert Decimal128 fields to float
            for k, v in list(doc.items()):
                if isinstance(v, Decimal128):
                    doc[k] = float(v.to_decimal())
            # Convert Timestamp to ISO string
            if isinstance(doc.get("Timestamp"), datetime):
                doc["Timestamp"] = doc["Timestamp"].isoformat()
            results.append(doc)

        client.close()
        return jsonify(results), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@consumerApi.route('/by-dtr/<string:dtr_id>', methods=['GET'])
def get_consumers_by_dtr(dtr_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
                       SELECT ConsumerID    as consumer_id,
                              Name          as name,
                              Consumer_type as type,
                              Address       as address,
                              District      as district,
                              DTR_id        as dtr_id,
                              PinCode       as pincode
                       FROM consumers_details
                       WHERE DTR_id = %s
                       """, (dtr_id,))
        consumers = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(consumers), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@consumerApi.route('/', methods=['GET'])
def get_all_consumers():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
                       SELECT ConsumerID    as consumer_id,
                              Name          as name,
                              Consumer_type as type,
                              Address       as address,
                              District      as district,
                              DTR_id        as dtr_id,
                              PinCode       as pincode
                       FROM consumers_details
                       """)
        consumers = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(consumers), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── CREATE ───────────────────────────────────────────────────────

@consumerApi.route('/', methods=['POST'])
def create_consumer():
    data = request.get_json()
    required = ['consumer_id', 'name']
    if not all(field in data for field in required):
        return jsonify({"error": "consumer_id and name are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
                       INSERT INTO consumers_details
                           (ConsumerID, Name, Consumer_type, Address, District, PinCode, DTR_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       """, (
                           data['consumer_id'],
                           data['name'],
                           data.get('type'),
                           data.get('address'),
                           data.get('district'),
                           data.get('pincode'),
                           data.get('dtr_id')
                       ))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Consumer created"}), 201

    except mysql.connector.Error as err:
        # duplicate PK will be errno=1062
        return jsonify({"error": str(err)}), 500


# ─── UPDATE ───────────────────────────────────────────────────────

@consumerApi.route('/<string:consumer_id>', methods=['PUT'])
def update_consumer(consumer_id):
    data = request.get_json()
    allowed = ['name', 'type', 'address', 'district', 'pincode', 'dtr_id']
    fields = []
    vals = []
    for f in allowed:
        if f in data:
            # map Python key to SQL column name
            col = 'Name' if f == 'name' else \
                'Consumer_type' if f == 'type' else \
                    'Address' if f == 'address' else \
                        'District' if f == 'district' else \
                            'PinCode' if f == 'pincode' else \
                                'DTR_id'
            fields.append(f"{col} = %s")
            vals.append(data[f])
    if not fields:
        return jsonify({"error": "No valid fields to update"}), 400

    vals.append(consumer_id)  # for WHERE
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE consumers_details SET {', '.join(fields)} WHERE ConsumerID = %s",
            tuple(vals)
        )
        conn.commit()
        if cursor.rowcount:
            res = jsonify({"message": "Consumer updated"}), 200
        else:
            res = jsonify({"error": "Not found"}), 404
        cursor.close()
        conn.close()
        return res

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── DELETE ───────────────────────────────────────────────────────

@consumerApi.route('/<string:consumer_id>', methods=['DELETE'])
def delete_consumer(consumer_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM consumers_details WHERE ConsumerID = %s",
            (consumer_id,)
        )
        conn.commit()
        if cursor.rowcount:
            res = jsonify({"message": "Consumer deleted"}), 200
        else:
            res = jsonify({"error": "Not found"}), 404
        cursor.close()
        conn.close()
        return res

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

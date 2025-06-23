from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os
import json
from bson.decimal128 import Decimal128
from datetime import datetime
from pymongo import MongoClient

# Create a Blueprint
feederApi = Blueprint('feeder', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0],
}


@feederApi.route('/consumption', methods=['GET'])
def get_feeders_from_mongo():
    """
    Fetch Feeder docs from MongoDB collection `powercasting.Feeder`
    Filters:
      • start_date, end_date (required, ISO 8601)
      • feeder_id       (optional)
    """
    # 1️⃣ Time-range params (required)
    start_str = request.args.get('start_date')
    end_str = request.args.get('end_date')
    if not start_str or not end_str:
        return jsonify({
            "error": "start_date and end_date query parameters are required (ISO format)."
        }), 400

    # 2️⃣ Parse into datetime
    try:
        start = datetime.fromisoformat(start_str.rstrip('Z'))
        end = datetime.fromisoformat(end_str.rstrip('Z'))
    except ValueError:
        return jsonify({
            "error": "Invalid date format. Use ISO 8601, e.g. 2023-04-01T00:00:00"
        }), 400

    # 3️⃣ Optional feeder_id param
    feeder_id = request.args.get('feeder_id')

    try:
        # 4️⃣ Connect to Mongo using your MONGO_URI env var
        client = MongoClient(os.getenv("MONGO_URI"))
        coll = client["powercasting"]["Feeder"]

        # 5️⃣ Build query
        query = {
            "Timestamp": {"$gte": start, "$lte": end}
        }
        if feeder_id:
            query["FEEDER_id"] = feeder_id

        # 6️⃣ Fetch & convert
        cursor = coll.find(query, {"_id": False})
        results = []
        for doc in cursor:
            # Decimal128 → float
            for k, v in list(doc.items()):
                if isinstance(v, Decimal128):
                    doc[k] = float(v.to_decimal())
            # datetime → ISO string
            ts = doc.get("Timestamp")
            if isinstance(ts, datetime):
                doc["Timestamp"] = ts.isoformat()
            results.append(doc)

        client.close()
        return jsonify(results), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@feederApi.route('/all', methods=['GET'])
def get_all_feeder_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM feeder")
        feeder_data = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(feeder_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@feederApi.route('/<int:id>', methods=['GET'])
def get_feeder_by_id(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM feeder_power_theft WHERE id = %s", (id,))
        feeder_data = cursor.fetchone()

        cursor.close()
        conn.close()

        if feeder_data:
            return jsonify(feeder_data), 200
        return jsonify({"error": "Feeder record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@feederApi.route('/', methods=['POST'])
def create_feeder_record():
    data = request.json
    required_fields = ['feeder_id', 'substation_id', 'feeder_name']

    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""            INSERT INTO feeder
                                          (feeder_id, substation_id, feeder_name)
                                      VALUES (%s, %s, %s)
                       """, (data['feeder_id'], data['substation_id'], data['feeder_name']))

        conn.commit()
        new_id = cursor.lastrowid

        cursor.close()
        conn.close()

        return jsonify({"message": "Record created successfully", "id": new_id}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@feederApi.route('/<int:id>', methods=['PUT'])
def update_feeder_record(id):
    data = request.json
    updateable_fields = ['date', 'feeder_name', 'units_assessed', 'amount_assessed', 'amount_realized']

    if not any(field in data for field in updateable_fields):
        return jsonify({"error": "No valid fields to update"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Build update query dynamically based on provided fields
        update_fields = []
        update_values = []
        for field in updateable_fields:
            if field in data:
                update_fields.append(f"{field} = %s")
                update_values.append(data[field])

        update_values.append(id)  # Add id for WHERE clause
        update_query = f"""
            UPDATE feeder_power_theft 
            SET {', '.join(update_fields)}
            WHERE id = %s
        """

        cursor.execute(update_query, tuple(update_values))
        conn.commit()

        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()

        if affected_rows > 0:
            return jsonify({"message": "Record updated successfully"}), 200
        return jsonify({"error": "Record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@feederApi.route('/<int:id>', methods=['DELETE'])
def delete_feeder_record(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("DELETE FROM feeder_power_theft WHERE id = %s", (id,))
        conn.commit()

        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()

        if affected_rows > 0:
            return jsonify({"message": "Record deleted successfully"}), 200
        return jsonify({"error": "Record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@feederApi.route('/by-substation/<substation_id>', methods=['GET'])
def get_feeders_by_substation(substation_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM feeder WHERE substation_id = %s", (substation_id,))
        feeders = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify({"status": "success", "data": feeders}), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

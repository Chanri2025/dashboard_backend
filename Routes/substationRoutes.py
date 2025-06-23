from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from datetime import datetime
import json

# Create a Blueprint
substationApi = Blueprint('substation', __name__)

# load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0]  # Using guvnl_consumers for substation routes,
}


@substationApi.route('/consumption', methods=['GET'])
def get_substation_consumption_from_mongo():
    """
    Fetch Substation consumption docs from MongoDB collection `powercasting.Substation`.
    Filters:
      • start_date, end_date   (required, ISO 8601)
      • substation_id          (optional)
    """
    # 1️⃣ Time-range (required)
    start_str = request.args.get('start_date')
    end_str = request.args.get('end_date')
    if not start_str or not end_str:
        return jsonify({
            "error": "Both start_date and end_date are required (ISO format)."
        }), 400

    # 2️⃣ Parse into datetime
    try:
        start = datetime.fromisoformat(start_str.rstrip('Z'))
        end = datetime.fromisoformat(end_str.rstrip('Z'))
    except ValueError:
        return jsonify({
            "error": "Invalid date format. Use ISO 8601, e.g. 2023-04-01T00:00:00"
        }), 400

    # 3️⃣ Optional substation_id
    substation_id = request.args.get('substation_id')

    try:
        # 4️⃣ Connect to Mongo via MONGO_URI
        client = MongoClient(os.getenv("MONGO_URI"))
        coll = client["powercasting"]["Substation"]

        # 5️⃣ Build query
        query = {"Timestamp": {"$gte": start, "$lte": end}}
        if substation_id:
            query["SUBSTATION_id"] = substation_id

        # 6️⃣ Fetch & convert
        cursor = coll.find(query, {"_id": False})
        results = []
        for doc in cursor:
            # convert Decimal128 → float
            for k, v in list(doc.items()):
                if isinstance(v, Decimal128):
                    doc[k] = float(v.to_decimal())
            # convert datetime → ISO string
            ts = doc.get("Timestamp")
            if isinstance(ts, datetime):
                doc["Timestamp"] = ts.isoformat()
            results.append(doc)

        client.close()
        return jsonify(results), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@substationApi.route('/all', methods=['GET'])
def get_all_substation_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM substation")
        substation_data = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(substation_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@substationApi.route('/<int:id>', methods=['GET'])
def get_substation_by_id(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM substation_power_theft WHERE id = %s", (id,))
        substation_data = cursor.fetchone()

        cursor.close()
        conn.close()

        if substation_data:
            return jsonify(substation_data), 200
        return jsonify({"error": "Substation record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@substationApi.route('/', methods=['POST'])
def create_substation_record():
    data = request.json
    required_fields = ['substation_id', 'division_id', 'substation_name', 'capacity_kva', 'primary_voltage']

    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""            INSERT INTO substation
                                      (substation_id, division_id, substation_name, capacity_kva, primary_voltage)
                                      VALUES (%s, %s, %s, %s, %s)
                       """, (data['substation_id'], data['division_id'], data['substation_name'],
                             data['capacity_kva'], data['primary_voltage']))

        conn.commit()
        new_id = cursor.lastrowid

        cursor.close()
        conn.close()

        return jsonify({"message": "Record created successfully", "id": new_id}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@substationApi.route('/<int:id>', methods=['PUT'])
def update_substation_record(id):
    data = request.json
    updateable_fields = ['date', 'substation_name', 'units_assessed', 'amount_assessed', 'amount_realized']

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
            UPDATE substation_power_theft 
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


@substationApi.route('/<int:id>', methods=['DELETE'])
def delete_substation_record(id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("DELETE FROM substation_power_theft WHERE id = %s", (id,))
        conn.commit()

        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()

        if affected_rows > 0:
            return jsonify({"message": "Record deleted successfully"}), 200
        return jsonify({"error": "Record not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@substationApi.route('/stats', methods=['GET'])
def get_substation_stats():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Get summary statistics
        cursor.execute("""
                       SELECT COUNT(*)                        as total_cases,
                              SUM(units_assessed)             as total_units_assessed,
                              AVG(amount_assessed)            as avg_amount_assessed,
                              SUM(amount_realized)            as total_amount_realized,
                              COUNT(DISTINCT substation_name) as total_substations
                       FROM substation_power_theft
                       """)
        stats = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify(stats), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@substationApi.route('/by-substation', methods=['GET'])
def get_by_substation():
    substation_name = request.args.get('substation_name')

    if not substation_name:
        return jsonify({"error": "substation_name parameter is required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
                       SELECT *
                       FROM substation_power_theft
                       WHERE substation_name = %s
                       """, (substation_name,))

        substation_data = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(substation_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@substationApi.route('/high-risk', methods=['GET'])
def get_high_risk_substations():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
                       SELECT substation_name,
                              COUNT(*)             as theft_cases,
                              SUM(units_assessed)  as total_units_assessed,
                              SUM(amount_assessed) as total_amount_assessed
                       FROM substation_power_theft
                       GROUP BY substation_name
                       HAVING COUNT(*) > 5
                       ORDER BY theft_cases DESC
                       """)

        high_risk_data = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(high_risk_data), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


@substationApi.route('/by-division/<division_id>', methods=['GET'])
def get_substations_by_division(division_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM substation WHERE division_id = %s", (division_id,))
        substations = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(substations), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

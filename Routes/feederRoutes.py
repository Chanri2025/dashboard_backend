from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os
from bson.decimal128 import Decimal128
from datetime import datetime
from pymongo import MongoClient

load_dotenv()
feederApi = Blueprint('feeder', __name__)

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0],
}


# ─── Mongo “consumption” ───────────────────────────────────────────
@feederApi.route('/consumption', methods=['GET'])
def get_feeders_from_mongo():
    start_str = request.args.get('start_date')
    end_str = request.args.get('end_date')
    if not start_str or not end_str:
        return jsonify({"error": "start_date and end_date are required"}), 400

    try:
        start = datetime.fromisoformat(start_str.rstrip('Z'))
        end = datetime.fromisoformat(end_str.rstrip('Z'))
    except ValueError:
        return jsonify({"error": "Use ISO8601, e.g. 2023-04-01T00:00:00"}), 400

    feeder_id = request.args.get('feeder_id')

    try:
        client = MongoClient(os.getenv('MONGO_URI'))
        coll = client["powercasting"]["Feeder"]
        query = {"Timestamp": {"$gte": start, "$lte": end}}
        if feeder_id:
            query["feeder_id"] = feeder_id

        docs = []
        for doc in coll.find(query, {"_id": False}):
            for k, v in list(doc.items()):
                if isinstance(v, Decimal128):
                    doc[k] = float(v.to_decimal())
            if isinstance(doc.get("Timestamp"), datetime):
                doc["Timestamp"] = doc["Timestamp"].isoformat()
            docs.append(doc)

        client.close()
        return jsonify(docs), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── READ ALL ───────────────────────────────────────────────────────
@feederApi.route('/', methods=['GET'])
def get_all_feeders():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM feeder")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── READ ONE ───────────────────────────────────────────────────────
@feederApi.route('/<string:feeder_id>', methods=['GET'])
def get_feeder(feeder_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM feeder WHERE feeder_id = %s", (feeder_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return jsonify(row), 200
        return jsonify({"error": "Not found"}), 404
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── CREATE ─────────────────────────────────────────────────────────
@feederApi.route('/', methods=['POST'])
def create_feeder():
    data = request.get_json() or {}
    # now only these two fields are required
    if 'substation_id' not in data or 'feeder_name' not in data:
        return jsonify({"error": "substation_id and feeder_name are required"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 1) Compute next feeder sequence:
        cursor.execute("""
                       SELECT MAX(
                                      CAST(
                                              SUBSTRING_INDEX(feeder_id, 'FEEDER', -1)
                                          AS UNSIGNED
                                      )
                              ) AS max_seq
                       FROM feeder
                       WHERE feeder_id LIKE 'FEEDER%%'
                       """)
        row = cursor.fetchone()
        max_seq = row['max_seq'] or 0
        new_id = f"FEEDER{max_seq + 1}"

        # 2) Insert with auto-generated ID
        cursor.execute("""
                       INSERT INTO feeder
                           (feeder_id, substation_id, feeder_name, capacity_amperes)
                       VALUES (%s, %s, %s, %s)
                       """, (
                           new_id,
                           data['substation_id'],
                           data['feeder_name'],
                           data.get('capacity_amperes')
                       ))
        conn.commit()

        cursor.close()
        conn.close()

        # 3) Return the new feeder_id
        return jsonify({
            "message": "Feeder created",
            "feeder_id": new_id
        }), 201

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── UPDATE ─────────────────────────────────────────────────────────
@feederApi.route('/<string:feeder_id>', methods=['PUT'])
def update_feeder(feeder_id):
    data = request.get_json() or {}
    allowed = ['substation_id', 'feeder_name', 'capacity_amperes']
    sets, vals = [], []
    for f in allowed:
        if f in data:
            sets.append(f"{f} = %s")
            vals.append(data[f])
    if not sets:
        return jsonify({"error": "No valid fields to update"}), 400

    vals.append(feeder_id)
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE feeder SET {', '.join(sets)} WHERE feeder_id = %s",
            tuple(vals)
        )
        conn.commit()
        rc = cursor.rowcount
        cursor.close()
        conn.close()
        if rc:
            return jsonify({"message": "Updated"}), 200
        return jsonify({"error": "Not found"}), 404

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── DELETE ─────────────────────────────────────────────────────────
@feederApi.route('/<string:feeder_id>', methods=['DELETE'])
def delete_feeder(feeder_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM feeder WHERE feeder_id = %s", (feeder_id,))
        conn.commit()
        rc = cursor.rowcount
        cursor.close()
        conn.close()
        if rc:
            return jsonify({"message": "Deleted"}), 200
        return jsonify({"error": "Not found"}), 404

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── BY-SUBSTATION ──────────────────────────────────────────────────
@feederApi.route('/by-substation/<string:substation_id>', methods=['GET'])
def get_by_substation(substation_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM feeder WHERE substation_id = %s",
                       (substation_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

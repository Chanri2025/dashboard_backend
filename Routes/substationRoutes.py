from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from datetime import datetime

load_dotenv()
substationApi = Blueprint('substation', __name__)

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0],
}


# ─── Mongo “consumption” ───────────────────────────────────────────
@substationApi.route('/consumption', methods=['GET'])
def get_substation_consumption_from_mongo():
    start_str = request.args.get('start_date')
    end_str = request.args.get('end_date')
    if not start_str or not end_str:
        return jsonify({"error": "Both start_date and end_date are required"}), 400

    try:
        start = datetime.fromisoformat(start_str.rstrip('Z'))
        end = datetime.fromisoformat(end_str.rstrip('Z'))
    except ValueError:
        return jsonify({"error": "Invalid ISO 8601 format"}), 400

    substation_id = request.args.get('substation_id')
    try:
        client = MongoClient(os.getenv('MONGO_URI'))
        coll = client["powercasting"]["Substation"]

        query = {"Timestamp": {"$gte": start, "$lte": end}}
        if substation_id:
            query["Substation_id"] = substation_id

        docs = []
        for doc in coll.find(query, {'_id': False}):
            for k, v in list(doc.items()):
                if isinstance(v, Decimal128):
                    doc[k] = float(v.to_decimal())
            ts = doc.get("Timestamp")
            if isinstance(ts, datetime):
                doc["Timestamp"] = ts.isoformat()
            docs.append(doc)

        client.close()
        return jsonify(docs), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── READ ALL ───────────────────────────────────────────────────────
@substationApi.route('/', methods=['GET'])
def get_all_substations():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM substation")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows), 200

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── READ ONE ───────────────────────────────────────────────────────
@substationApi.route('/<string:substation_id>', methods=['GET'])
def get_substation(substation_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM substation WHERE substation_id = %s",
            (substation_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return jsonify(row), 200
        return jsonify({"error": "Not found"}), 404

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── CREATE ─────────────────────────────────────────────────────────
@substationApi.route('/', methods=['POST'])
def create_substation():
    data = request.get_json() or {}
    required = ['substation_id', 'division_id', 'substation_name', 'capacity_kva', 'primary_voltage']
    if not all(f in data for f in required):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
                       INSERT INTO substation
                       (substation_id, division_id, substation_name, capacity_kva, primary_voltage)
                       VALUES (%s, %s, %s, %s, %s)
                       """, (
                           data['substation_id'],
                           data['division_id'],
                           data['substation_name'],
                           data['capacity_kva'],
                           data['primary_voltage']
                       ))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({
            "message": "Created",
            "substation_id": data['substation_id']
        }), 201

    except mysql.connector.Error as err:
        # handle duplicate-pk
        if err.errno == 1062:
            return jsonify({"error": "substation_id already exists"}), 400
        return jsonify({"error": str(err)}), 500


# ─── UPDATE ─────────────────────────────────────────────────────────
@substationApi.route('/<string:substation_id>', methods=['PUT'])
def update_substation(substation_id):
    data = request.get_json() or {}
    allowed = ['division_id', 'substation_name', 'capacity_kva', 'primary_voltage']
    sets, vals = [], []
    for f in allowed:
        if f in data:
            sets.append(f"{f} = %s")
            vals.append(data[f])
    if not sets:
        return jsonify({"error": "No valid fields to update"}), 400

    vals.append(substation_id)
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE substation SET {', '.join(sets)} WHERE substation_id = %s",
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
@substationApi.route('/<string:substation_id>', methods=['DELETE'])
def delete_substation(substation_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM substation WHERE substation_id = %s",
            (substation_id,)
        )
        conn.commit()
        rc = cursor.rowcount
        cursor.close()
        conn.close()
        if rc:
            return jsonify({"message": "Deleted"}), 200
        return jsonify({"error": "Not found"}), 404

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ─── BY DIVISION ────────────────────────────────────────────────────
@substationApi.route('/by-division/<string:division_id>', methods=['GET'])
def get_substations_by_division(division_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM substation WHERE division_id = %s",
            (division_id,)
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(rows), 200

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

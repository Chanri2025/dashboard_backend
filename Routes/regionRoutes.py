from flask import Blueprint, jsonify, request
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

regionApi = Blueprint('regionApi', __name__)


def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAMES').split(',')[0]
    )


# ─── READ ALL ────────────────────────────────────────────────────────
@regionApi.route('/', methods=['GET'])
def get_all_regions():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM region')
        regions = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(regions), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── READ ONE ────────────────────────────────────────────────────────
@regionApi.route('/<string:region_id>', methods=['GET'])
def get_region(region_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM region WHERE region_id = %s', (region_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return jsonify(row), 200
        return jsonify({'error': 'Region not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── CREATE ──────────────────────────────────────────────────────────
@regionApi.route('/', methods=['POST'])
def create_region():
    data = request.get_json() or {}
    if 'region_name' not in data:
        return jsonify({'error': 'region_name is required'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # 1) Grab the last region_id: e.g. 'R005'
        cursor.execute("SELECT region_id FROM region ORDER BY region_id DESC LIMIT 1")
        last = cursor.fetchone()

        if last:
            # strip leading 'R', parse integer, add 1
            num = int(last['region_id'][1:]) + 1
        else:
            num = 1

        # 2) Build new ID, zero-padded to 3 digits: 'R006'
        new_id = f"R{num:03d}"

        # 3) Insert with the generated ID
        cursor.execute("""
                       INSERT INTO region (region_id, region_name, operational_contact)
                       VALUES (%s, %s, %s)
                       """, (
                           new_id,
                           data['region_name'],
                           data.get('operational_contact')
                       ))
        conn.commit()

        cursor.close()
        conn.close()

        # 4) Return the new ID back to the client
        return jsonify({
            'message': 'Region created',
            'region_id': new_id
        }), 201

    except mysql.connector.Error as err:
        # Duplicate-key should never happen, but catch others
        return jsonify({'error': str(err)}), 500


# ─── UPDATE ──────────────────────────────────────────────────────────
@regionApi.route('/<string:region_id>', methods=['PUT'])
def update_region(region_id):
    data = request.get_json()
    # only these fields may be updated
    allowed = ['region_name', 'operational_contact']
    updates = []
    vals = []
    for field in allowed:
        if field in data:
            updates.append(f"{field} = %s")
            vals.append(data[field])

    if not updates:
        return jsonify({'error': 'No valid fields to update'}), 400

    vals.append(region_id)  # for WHERE clause
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE region SET {', '.join(updates)} WHERE region_id = %s",
            tuple(vals)
        )
        conn.commit()
        if cursor.rowcount:
            res = jsonify({'message': 'Region updated'}), 200
        else:
            res = jsonify({'error': 'Region not found'}), 404
        cursor.close()
        conn.close()
        return res

    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500


# ─── DELETE ──────────────────────────────────────────────────────────
@regionApi.route('/<string:region_id>', methods=['DELETE'])
def delete_region(region_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM region WHERE region_id = %s', (region_id,))
        conn.commit()
        if cursor.rowcount:
            res = jsonify({'message': 'Region deleted'}), 200
        else:
            res = jsonify({'error': 'Region not found'}), 404
        cursor.close()
        conn.close()
        return res

    except mysql.connector.Error as err:
        return jsonify({'error': str(err)}), 500

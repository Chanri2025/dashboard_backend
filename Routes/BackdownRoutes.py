from flask import Blueprint, jsonify, request
import mysql.connector
from dotenv import load_dotenv
import os

# Create a Blueprint
backDownApi = Blueprint('backdown', __name__)

# Load .env
load_dotenv()

# MySQL configuration from env
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1]
}


# ───── READ ─────
@backDownApi.route('/', methods=['GET'])
def get_backDown_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM `back_down_table`")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(data)
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ───── CREATE ─────
@backDownApi.route('/', methods=['POST'])
def add_backDown_entry():
    data = request.get_json()
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        query = """
                INSERT INTO back_down_table (Start_Load, End_Load, SHR, Aux_Consumption)
                VALUES (%s, %s, %s, %s) \
                """
        cursor.execute(query, (
            data['Start_Load'],
            data['End_Load'],
            data['SHR'],
            data['Aux_Consumption']
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Entry added successfully"}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ───── UPDATE ─────
@backDownApi.route('/<int:start_load>', methods=['PUT'])
def update_backDown_entry(start_load):
    data = request.get_json()
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        query = """
                UPDATE back_down_table
                SET End_Load        = %s,
                    SHR             = %s,
                    Aux_Consumption = %s
                WHERE Start_Load = %s \
                """
        cursor.execute(query, (
            data['End_Load'],
            data['SHR'],
            data['Aux_Consumption'],
            start_load
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Entry updated successfully"})
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500


# ───── DELETE ─────
@backDownApi.route('/<int:start_load>', methods=['DELETE'])
def delete_backDown_entry(start_load):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM back_down_table WHERE Start_Load = %s", (start_load,))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "Entry deleted successfully"})
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

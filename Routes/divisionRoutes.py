from flask import Blueprint, jsonify
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

divisionApi = Blueprint('divisionApi', __name__)

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAMES').split(',')[0]  # Using guvnl_consumers for division routes
    )

@divisionApi.route('/all', methods=['GET'])
def get_all_divisions():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM division')
        divisions = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(divisions), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@divisionApi.route('/by-region/<region_id>', methods=['GET'])
def get_divisions_by_region(region_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute('SELECT * FROM division WHERE region_id = %s', (region_id,))
        divisions = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify(divisions), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

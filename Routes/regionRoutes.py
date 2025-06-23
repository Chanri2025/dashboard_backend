from flask import Blueprint, jsonify
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
        database=os.getenv('DB_NAMES').split(',')[0]  # Using guvnl_consumers for region routes
    )

@regionApi.route('/all', methods=['GET'])
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

# routes/availability.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1],
}


def get_db_connection():
    return mysql.connector.connect(**db_config)


class PAFRecord(BaseModel):
    Code: str
    name: str
    Jan: float
    Feb: float
    Mar: float
    Apr: float
    May: float
    Jun: float
    Jul: float
    Aug: float
    Sep: float
    Oct: float
    Nov: float
    Dec: float


@router.get("/")
def get_availability():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM paf_details")
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


@router.post("/")
def add_availability(record: PAFRecord):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO PAF_Details
            (Code, name, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = tuple(record.dict().values())
        cursor.execute(query, values)
        conn.commit()
        return {"message": "Record inserted"}
    finally:
        cursor.close()
        conn.close()

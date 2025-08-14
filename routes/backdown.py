from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MySQL DB config
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1]
}

# FastAPI Router
router = APIRouter()


# ───── Pydantic Schemas ─────
class BackDownEntry(BaseModel):
    Start_Load: int
    End_Load: float  # Changed from int to float to avoid validation error
    SHR: float
    Aux_Consumption: float


class BackDownUpdate(BaseModel):
    End_Load: float  # Changed from int to float
    SHR: float
    Aux_Consumption: float


# ───── GET all entries ─────
@router.get("/", response_model=List[BackDownEntry])
def get_backdown_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM `back_down_table`")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ───── POST new entry ─────
@router.post("/", status_code=201)
def add_backdown_entry(entry: BackDownEntry):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = """
            INSERT INTO back_down_table (Start_Load, End_Load, SHR, Aux_Consumption)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (
            entry.Start_Load,
            entry.End_Load,
            entry.SHR,
            entry.Aux_Consumption
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Entry added successfully"}
    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ───── PUT update entry ─────
@router.put("/{start_load}")
def update_backdown_entry(start_load: int, update: BackDownUpdate):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = """
            UPDATE back_down_table
            SET End_Load = %s, SHR = %s, Aux_Consumption = %s
            WHERE Start_Load = %s
        """
        cursor.execute(query, (
            update.End_Load,
            update.SHR,
            update.Aux_Consumption,
            start_load
        ))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Entry updated successfully"}
    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ───── DELETE entry ─────
@router.delete("/{start_load}")
def delete_backdown_entry(start_load: int):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM back_down_table WHERE Start_Load = %s", (start_load,))
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Entry deleted successfully"}
    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))

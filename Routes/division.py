from fastapi import APIRouter, HTTPException
from typing import List
import mysql.connector
from pydantic import BaseModel
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()


# ───── DB Connection ─────
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAMES").split(",")[0]  # guvnl_consumers
    )


# ───── Response Model ─────
class Division(BaseModel):
    id: int
    name: str
    region_id: int


# ───── Get All Divisions ─────
@router.get("/all", response_model=List[Division])
def get_all_divisions():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM division")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ───── Get Divisions by Region ID ─────
@router.get("/by-region/{region_id}", response_model=List[Division])
def get_divisions_by_region(region_id: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM division WHERE region_id = %s", (region_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

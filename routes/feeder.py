from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv
import os

# Load env
load_dotenv()

router = APIRouter()

# MySQL config
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0],
}

# Mongo URI
mongo_uri = os.getenv("MONGO_URI")


# ──────── Pydantic Models ─────────────
class FeederBase(BaseModel):
    substation_id: str
    feeder_name: str
    capacity_amperes: Optional[float] = None


class FeederEntry(FeederBase):
    feeder_id: str


# ──────── Mongo GET ───────────────────
@router.get("/consumption")
def get_feeders_from_mongo(start_date: str = Query(...), end_date: str = Query(...), feeder_id: Optional[str] = None):
    try:
        start = datetime.fromisoformat(start_date.rstrip('Z'))
        end = datetime.fromisoformat(end_date.rstrip('Z'))
    except ValueError:
        raise HTTPException(status_code=400, detail="Use ISO8601 format, e.g. 2023-04-01T00:00:00")

    try:
        client = MongoClient(mongo_uri)
        coll = client["powercasting"]["Feeder"]

        query = {"Timestamp": {"$gte": start, "$lte": end}}
        if feeder_id:
            query["FEEDER_ID"] = feeder_id

        results = []
        for doc in coll.find(query, {"_id": False}):
            for k, v in doc.items():
                if isinstance(v, Decimal128):
                    doc[k] = float(v.to_decimal())
            if isinstance(doc.get("Timestamp"), datetime):
                doc["Timestamp"] = doc["Timestamp"].isoformat()
            results.append(doc)

        client.close()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────── GET ALL ─────────────────────
@router.get("/", response_model=List[FeederEntry])
def get_all_feeders():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM feeder")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ──────── GET BY ID ───────────────────
@router.get("/{feeder_id}", response_model=FeederEntry)
def get_feeder(feeder_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM feeder WHERE feeder_id = %s", (feeder_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return row
        raise HTTPException(status_code=404, detail="Not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ──────── CREATE ──────────────────────
@router.post("/")
def create_feeder(data: FeederBase):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Get next sequence
        cursor.execute("""
            SELECT MAX(CAST(SUBSTRING_INDEX(feeder_id, 'FEEDER', -1) AS UNSIGNED)) AS max_seq
            FROM feeder
            WHERE feeder_id LIKE 'FEEDER%%'
        """)
        row = cursor.fetchone()
        next_seq = (row['max_seq'] or 0) + 1
        new_id = f"FEEDER{next_seq}"

        cursor.execute("""
            INSERT INTO feeder (feeder_id, substation_id, feeder_name, capacity_amperes)
            VALUES (%s, %s, %s, %s)
        """, (new_id, data.substation_id, data.feeder_name, data.capacity_amperes))

        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Feeder created", "feeder_id": new_id}
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ──────── UPDATE ──────────────────────
@router.put("/{feeder_id}")
def update_feeder(feeder_id: str, data: FeederBase):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE feeder 
            SET substation_id = %s, feeder_name = %s, capacity_amperes = %s
            WHERE feeder_id = %s
        """, (data.substation_id, data.feeder_name, data.capacity_amperes, feeder_id))
        conn.commit()
        rc = cursor.rowcount
        cursor.close()
        conn.close()
        if rc:
            return {"message": "Updated"}
        raise HTTPException(status_code=404, detail="Not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ──────── DELETE ──────────────────────
@router.delete("/{feeder_id}")
def delete_feeder(feeder_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM feeder WHERE feeder_id = %s", (feeder_id,))
        conn.commit()
        rc = cursor.rowcount
        cursor.close()
        conn.close()
        if rc:
            return {"message": "Deleted"}
        raise HTTPException(status_code=404, detail="Not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


# ──────── BY SUBSTATION ───────────────
@router.get("/by-substation/{substation_id}", response_model=List[FeederEntry])
def get_feeder_by_substation(substation_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM feeder WHERE substation_id = %s", (substation_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))

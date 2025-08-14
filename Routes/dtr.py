from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from datetime import datetime, date
import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
router = APIRouter()

# MySQL DB config
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[0]
}

# Mongo config
mongo_uri = os.getenv("MONGO_URI")


# Pydantic Models
class DTRBase(BaseModel):
    feeder_id: str
    location_description: str
    capacity_kva: float
    residential_connections: int
    installed_date: date  # changed from str to date


class DTREntry(DTRBase):
    dtr_id: str


class DTRStats(BaseModel):
    total_dtrs: int
    total_feeders: int
    total_capacity: float
    avg_capacity: float
    total_connections: int


# Mongo Endpoint
@router.get("/consumption")
def get_dtr_from_mongo(start_date: str = Query(...), end_date: str = Query(...), dtr_id: Optional[str] = None):
    try:
        start = datetime.fromisoformat(start_date.rstrip('Z'))
        end = datetime.fromisoformat(end_date.rstrip('Z'))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use ISO 8601.")

    try:
        client = MongoClient(mongo_uri)
        coll = client["powercasting"]["DTR"]

        query = {"Timestamp": {"$gte": start, "$lte": end}}
        if dtr_id:
            query["DTR_ID"] = dtr_id

        cursor = coll.find(query, {"_id": False})
        results = []
        for doc in cursor:
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


# SQL Endpoints
@router.get("/", response_model=List[DTREntry])
def get_all_dtr():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT d.*, f.feeder_name FROM dtr d
            LEFT JOIN feeder f ON d.feeder_id = f.feeder_id
        """)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.get("/{dtr_id}", response_model=DTREntry)
def get_dtr_by_id(dtr_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT d.*, f.feeder_name FROM dtr d
            LEFT JOIN feeder f ON d.feeder_id = f.feeder_id
            WHERE d.dtr_id = %s
        """, (dtr_id,))
        data = cursor.fetchone()
        cursor.close()
        conn.close()
        if not data:
            raise HTTPException(status_code=404, detail="Record not found")
        return data
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.get("/by-feeder/{feeder_id}", response_model=List[DTREntry])
def get_dtr_by_feeder(feeder_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT d.*, f.feeder_name FROM dtr d
            LEFT JOIN feeder f ON d.feeder_id = f.feeder_id
            WHERE d.feeder_id = %s
        """, (feeder_id,))
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.get("/stats", response_model=DTRStats)
def get_dtr_stats():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT COUNT(*) AS total_dtrs,
                   COUNT(DISTINCT feeder_id) AS total_feeders,
                   SUM(capacity_kva) AS total_capacity,
                   AVG(capacity_kva) AS avg_capacity,
                   SUM(residential_connections) AS total_connections
            FROM dtr
        """)
        stats = cursor.fetchone()
        cursor.close()
        conn.close()

        if not stats:
            raise HTTPException(status_code=404, detail="Record not found")

        # Ensure all fields are present and have default values if None
        return {
            "total_dtrs": stats["total_dtrs"] or 0,
            "total_feeders": stats["total_feeders"] or 0,
            "total_capacity": float(stats["total_capacity"] or 0),
            "avg_capacity": float(stats["avg_capacity"] or 0),
            "total_connections": stats["total_connections"] or 0
        }

    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.post("/")
def create_dtr(dtr: DTRBase):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT 1 FROM feeder WHERE feeder_id = %s", (dtr.feeder_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=400, detail="Invalid feeder_id")

        cursor.execute("""
            SELECT MAX(CAST(SUBSTRING_INDEX(dtr_id, '_DTR', -1) AS UNSIGNED)) AS max_seq
            FROM dtr WHERE feeder_id = %s
        """, (dtr.feeder_id,))
        row = cursor.fetchone()
        next_seq = (row['max_seq'] or 0) + 1
        new_id = f"{dtr.feeder_id}_DTR{next_seq}"

        cursor.execute("""
            INSERT INTO dtr (dtr_id, feeder_id, location_description, capacity_kva, residential_connections, installed_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (new_id, dtr.feeder_id, dtr.location_description, dtr.capacity_kva, dtr.residential_connections,
              dtr.installed_date))

        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Record created", "dtr_id": new_id}
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.put("/{dtr_id}")
def update_dtr(dtr_id: str, dtr: DTRBase):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT 1 FROM feeder WHERE feeder_id = %s", (dtr.feeder_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=400, detail="Invalid feeder_id")

        cursor.execute("""
           UPDATE dtr SET feeder_id = %s, location_description = %s, capacity_kva = %s,
                           residential_connections = %s, installed_date = %s
            WHERE dtr_id = %s
        """, (dtr.feeder_id, dtr.location_description, dtr.capacity_kva,
              dtr.residential_connections, dtr.installed_date, dtr_id))

        conn.commit()
        affected = cursor.rowcount
        cursor.close()
        conn.close()

        if affected:
            return {"message": "Record updated"}
        raise HTTPException(status_code=404, detail="Record not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.delete("/{dtr_id}")
def delete_dtr(dtr_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dtr WHERE dtr_id = %s", (dtr_id,))
        conn.commit()
        affected = cursor.rowcount
        cursor.close()
        conn.close()

        if affected:
            return {"message": "Record deleted"}
        raise HTTPException(status_code=404, detail="Record not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))

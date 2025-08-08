import os
from datetime import datetime
from typing import Optional, List, Any

import mysql.connector
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from dotenv import load_dotenv

from utils.date_utils import parse_start_timestamp, parse_end_timestamp

load_dotenv()

# IMPORTANT: no prefix here (prefix added in main.py)
router = APIRouter()

# ───────────────────────── DB CONFIG ─────────────────────────
db_config = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "database": "guvnl_consumers",
}
MONGO_URI = os.getenv("MONGO_URI")


# ──────────────────────── HELPERS ───────────────────────────
def convert_decimal128(obj: Any) -> Any:
    """Recursively convert Mongo Decimal128 -> float."""
    if isinstance(obj, dict):
        return {k: convert_decimal128(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal128(v) for v in obj]
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    return obj


# ──────────────────────── SCHEMAS ───────────────────────────
class Consumer(BaseModel):
    consumer_id: str
    name: str
    type: Optional[str] = None
    address: Optional[str] = None
    district: Optional[str] = None
    dtr_id: Optional[str] = None
    pincode: Optional[str] = None


class UpdateConsumer(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    address: Optional[str] = None
    district: Optional[str] = None
    dtr_id: Optional[str] = None
    pincode: Optional[str] = None


# ───────────────────────── ROUTES ───────────────────────────


@router.get("/consumption")
def get_lt_consumption_from_mongo(
    start_date: str = Query(..., description="YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]"),
    end_date: str = Query(..., description="YYYY-MM-DD or YYYY-MM-DD HH:MM[:SS]"),
    consumer_id: Optional[str] = Query(None),
):
    """Fetch consumer LT consumption from Mongo between start/end (inclusive)."""
    try:
        start = parse_start_timestamp(start_date)
        end = parse_end_timestamp(end_date)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    if start > end:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        client = MongoClient(MONGO_URI)
        coll = client["powercasting"]["Consumer_Consumption"]

        query = {"Timestamp": {"$gte": start, "$lte": end}}
        if consumer_id:
            query["Consumer_id"] = consumer_id.upper()

        results = []
        for doc in coll.find(query, {"_id": False}):
            doc = convert_decimal128(doc)
            if isinstance(doc.get("Timestamp"), datetime):
                doc["Timestamp"] = doc["Timestamp"].isoformat()
            results.append(doc)

        client.close()
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/by-dtr/{dtr_id}")
def get_consumers_by_dtr(dtr_id: str):
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT ConsumerID AS consumer_id,
                   Name       AS name,
                   Consumer_type AS type,
                   Address    AS address,
                   District   AS district,
                   DTR_id     AS dtr_id,
                   PinCode    AS pincode
            FROM consumers_details
            WHERE DTR_id = %s
            """,
            (dtr_id,),
        )
        return cur.fetchall()
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        if conn:
            conn.close()


@router.get("/", response_model=List[Consumer])
def get_all_consumers():
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT ConsumerID AS consumer_id,
                   Name       AS name,
                   Consumer_type AS type,
                   Address    AS address,
                   District   AS district,
                   DTR_id     AS dtr_id,
                   PinCode    AS pincode
            FROM consumers_details
            """
        )
        return cur.fetchall()
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        if conn:
            conn.close()


@router.post("/", status_code=201)
def create_consumer(data: Consumer):
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO consumers_details
                (ConsumerID, Name, Consumer_type, Address, District, PinCode, DTR_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data.consumer_id,
                data.name,
                data.type,
                data.address,
                data.district,
                data.pincode,
                data.dtr_id,
            ),
        )
        conn.commit()
        return {"message": "Consumer created"}
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        if conn:
            conn.close()


@router.put("/{consumer_id}")
def update_consumer(consumer_id: str, data: UpdateConsumer):
    mapping = {
        "name": "Name",
        "type": "Consumer_type",
        "address": "Address",
        "district": "District",
        "pincode": "PinCode",
        "dtr_id": "DTR_id",
    }

    updates = []
    values = []
    for field, col in mapping.items():
        val = getattr(data, field)
        if val is not None:
            updates.append(f"{col} = %s")
            values.append(val)

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    values.append(consumer_id)

    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor()
        cur.execute(
            f"UPDATE consumers_details SET {', '.join(updates)} WHERE ConsumerID = %s",
            tuple(values),
        )
        conn.commit()
        if cur.rowcount:
            return {"message": "Consumer updated"}
        raise HTTPException(status_code=404, detail="Consumer not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        if conn:
            conn.close()


@router.delete("/{consumer_id}")
def delete_consumer(consumer_id: str):
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM consumers_details WHERE ConsumerID = %s", (consumer_id,)
        )
        conn.commit()
        if cur.rowcount:
            return {"message": "Consumer deleted"}
        raise HTTPException(status_code=404, detail="Consumer not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        if conn:
            conn.close()

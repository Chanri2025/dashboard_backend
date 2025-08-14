import os
from datetime import datetime
from typing import List, Optional, Any

import mysql.connector
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from dotenv import load_dotenv

load_dotenv()

# No prefix here — you add it in main.py
router = APIRouter()

# ───────────── DB CONFIG ─────────────
db_config = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAMES").split(",")[0],
}
MONGO_URI = os.getenv("MONGO_URI")


# ───────────── Helpers ─────────────
def _parse_iso(s: str) -> datetime:
    """Accepts ISO 8601 like '2023-04-01T00:00:00Z' or with offset."""
    if not s:
        raise ValueError("missing date")
    s2 = s.rstrip("Z")
    if s.endswith("Z"):
        s2 += "+00:00"
    try:
        return datetime.fromisoformat(s2)
    except ValueError as e:
        raise ValueError(f"Invalid ISO 8601 format: {s}") from e


def _convert_decimal128(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _convert_decimal128(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_decimal128(v) for v in obj]
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    return obj


# ───────────── Schemas ─────────────
class SubstationBase(BaseModel):
    division_id: str = Field(..., examples=["DIV001"])
    substation_name: str = Field(..., examples=["Alpha Substation"])
    capacity_kva: int = Field(..., ge=0, examples=[1000])
    primary_voltage: str = Field(..., examples=["11kV"])


class SubstationCreate(SubstationBase):
    substation_id: str = Field(..., examples=["SS001"])


class SubstationUpdate(BaseModel):
    division_id: Optional[str] = None
    substation_name: Optional[str] = None
    capacity_kva: Optional[int] = Field(None, ge=0)
    primary_voltage: Optional[str] = None


class SubstationOut(SubstationBase):
    substation_id: str


# ───────────── Routes ─────────────


@router.get("/consumption")
def get_substation_consumption_from_mongo(
    start_date: str = Query(..., description="ISO 8601, e.g. 2023-04-01T00:00:00Z"),
    end_date: str = Query(..., description="ISO 8601, e.g. 2023-04-30T00:00:00Z"),
    substation_id: Optional[str] = Query(None),
):
    """Get substation consumption docs from Mongo within [start,end]."""
    try:
        start = _parse_iso(start_date)
        end = _parse_iso(end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        client = MongoClient(MONGO_URI)
        coll = client["powercasting"]["Substation"]

        query = {"Timestamp": {"$gte": start, "$lte": end}}
        if substation_id:
            query["Substation_id"] = substation_id

        out = []
        for doc in coll.find(query, {"_id": False}):
            doc = _convert_decimal128(doc)
            ts = doc.get("Timestamp")
            if isinstance(ts, datetime):
                doc["Timestamp"] = ts.isoformat()
            out.append(doc)
        client.close()
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[SubstationOut])
def get_all_substations():
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM substation")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.get("/{substation_id}", response_model=SubstationOut)
def get_substation(substation_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT * FROM substation WHERE substation_id = %s", (substation_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row
        raise HTTPException(status_code=404, detail="Not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.post("/", status_code=201)
def create_substation(data: SubstationCreate):
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO substation
            (substation_id, division_id, substation_name, capacity_kva, primary_voltage)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                data.substation_id,
                data.division_id,
                data.substation_name,
                data.capacity_kva,
                data.primary_voltage,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"message": "Created", "substation_id": data.substation_id}
    except mysql.connector.Error as err:
        # Duplicate primary key
        if getattr(err, "errno", None) == 1062:
            raise HTTPException(status_code=400, detail="substation_id already exists")
        raise HTTPException(status_code=500, detail=str(err))


@router.put("/{substation_id}")
def update_substation(substation_id: str, data: SubstationUpdate):
    allowed_map = {
        "division_id": "division_id",
        "substation_name": "substation_name",
        "capacity_kva": "capacity_kva",
        "primary_voltage": "primary_voltage",
    }
    updates, vals = [], []
    for field, col in allowed_map.items():
        val = getattr(data, field)
        if val is not None:
            updates.append(f"{col} = %s")
            vals.append(val)

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    vals.append(substation_id)
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor()
        cur.execute(
            f"UPDATE substation SET {', '.join(updates)} WHERE substation_id = %s",
            tuple(vals),
        )
        conn.commit()
        rc = cur.rowcount
        cur.close()
        conn.close()
        if rc:
            return {"message": "Updated"}
        raise HTTPException(status_code=404, detail="Not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.delete("/{substation_id}")
def delete_substation(substation_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor()
        cur.execute("DELETE FROM substation WHERE substation_id = %s", (substation_id,))
        conn.commit()
        rc = cur.rowcount
        cur.close()
        conn.close()
        if rc:
            return {"message": "Deleted"}
        raise HTTPException(status_code=404, detail="Not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.get("/by-division/{division_id}", response_model=List[SubstationOut])
def get_substations_by_division(division_id: str):
    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM substation WHERE division_id = %s", (division_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))

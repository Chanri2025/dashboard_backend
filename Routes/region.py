import os
from typing import List, Optional
import mysql.connector
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()  # prefix added in main.py

# ───────────── DB ─────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAMES").split(",")[0],
}


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


# ───────────── Schemas ─────────────
class RegionBase(BaseModel):
    region_name: str = Field(..., examples=["North Zone"])
    operational_contact: Optional[str] = Field(None, examples=["+91-9876543210"])


class RegionCreate(RegionBase):
    pass


class RegionUpdate(BaseModel):
    region_name: Optional[str] = None
    operational_contact: Optional[str] = None


class RegionOut(RegionBase):
    region_id: str = Field(..., examples=["R006"])


# ───────────── Routes ─────────────


@router.get("/", response_model=List[RegionOut])
def get_all_regions():
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM region")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{region_id}", response_model=RegionOut)
def get_region(region_id: str):
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM region WHERE region_id = %s", (region_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return row
        raise HTTPException(status_code=404, detail="Region not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", status_code=201)
def create_region(data: RegionCreate):
    # region_name is required by schema
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)

        # 1) Get last region_id, e.g. 'R005'
        cur.execute("SELECT region_id FROM region ORDER BY region_id DESC LIMIT 1")
        last = cur.fetchone()
        if (
            last
            and last["region_id"].startswith("R")
            and last["region_id"][1:].isdigit()
        ):
            num = int(last["region_id"][1:]) + 1
        else:
            num = 1

        # 2) Build new ID 'R006'
        new_id = f"R{num:03d}"

        # 3) Insert
        cur2 = conn.cursor()
        cur2.execute(
            """
            INSERT INTO region (region_id, region_name, operational_contact)
            VALUES (%s, %s, %s)
            """,
            (new_id, data.region_name, data.operational_contact),
        )
        conn.commit()
        cur2.close()
        cur.close()
        conn.close()

        # 4) Return minimal payload (like Flask)
        return {"message": "Region created", "region_id": new_id}
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.put("/{region_id}")
def update_region(region_id: str, data: RegionUpdate):
    # only allow certain fields
    updates = []
    vals = []
    if data.region_name is not None:
        updates.append("region_name = %s")
        vals.append(data.region_name)
    if data.operational_contact is not None:
        updates.append("operational_contact = %s")
        vals.append(data.operational_contact)

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    vals.append(region_id)
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            f"UPDATE region SET {', '.join(updates)} WHERE region_id = %s",
            tuple(vals),
        )
        conn.commit()
        rc = cur.rowcount
        cur.close()
        conn.close()
        if rc:
            return {"message": "Region updated"}
        raise HTTPException(status_code=404, detail="Region not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))


@router.delete("/{region_id}")
def delete_region(region_id: str):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM region WHERE region_id = %s", (region_id,))
        conn.commit()
        rc = cur.rowcount
        cur.close()
        conn.close()
        if rc:
            return {"message": "Region deleted"}
        raise HTTPException(status_code=404, detail="Region not found")
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=str(err))

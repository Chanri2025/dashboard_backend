from fastapi import APIRouter, HTTPException, Query, Body, Path
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
import mysql.connector
from pymongo import MongoClient
from dotenv import load_dotenv
import os


load_dotenv()

router = APIRouter()

# MySQL Configuration
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1]  # guvnl_plants
}

# MongoDB Setup
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(mongo_uri)
db = client["powercasting"]
collection = db["mustrunplantconsumption"]


class Plant(BaseModel):
    Name: str
    Code: str
    Ownership: str
    Fuel_Type: str
    Rated_Capacity: float
    PAF: float
    PLF: float
    Aux_Consumption: float
    Variable_Cost: float
    Type: str
    Technical_Minimum: float
    Max_Power: float
    Min_Power: float


@router.get("/all")
def get_all_plant_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM plant_details")
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
def get_plant_summary():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT name, Code, Rated_Capacity, PAF, PLF, Type,
                   Technical_Minimum, Aux_Consumption, Max_Power, Min_Power, Variable_Cost
            FROM plant_details WHERE Type = 'Must run'
        """)
        must_run = cursor.fetchall()

        cursor.execute("""
            SELECT name, Code, Rated_Capacity, PAF, PLF, Type,
                   Technical_Minimum, Aux_Consumption, Max_Power, Min_Power, Variable_Cost
            FROM plant_details WHERE Type = 'Other'
        """)
        other = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "must_run_count": len(must_run),
            "other_count": len(other),
            "must_run": must_run,
            "other": other
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{plant_name}")
def get_each_plant_data(
        plant_name: str = Path(...),
        start_date: str = Query(...),
        end_date: str = Query(...)
):
    try:
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)

        query = {
            "TimeStamp": {"$gte": start_dt, "$lte": end_dt},
            "Plant_Name": plant_name
        }

        docs = list(collection.find(query, {"_id": 0}))
        return docs

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
def add_plant(data: Plant):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO plant_details
            (name, Code, Ownership, Fuel_Type, Rated_Capacity, PAF, PLF,
             Aux_Consumption, Variable_Cost, Type, Technical_Minimum, Max_Power, Min_Power)
            VALUES (%(Name)s, %(Code)s, %(Ownership)s, %(Fuel_Type)s, %(Rated_Capacity)s,
                    %(PAF)s, %(PLF)s, %(Aux_Consumption)s, %(Variable_Cost)s,
                    %(Type)s, %(Technical_Minimum)s, %(Max_Power)s, %(Min_Power)s)
        """
        cursor.execute(insert_query, data.dict())
        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "Plant added successfully"}

    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"MySQL Error: {err}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {e}")


@router.put("/{plant_code}")
def update_plant_data(plant_code: str, data: Plant):
    if data.Code != plant_code:
        raise HTTPException(status_code=400, detail="Plant code mismatch between URL and body")

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        update_query = """
            UPDATE plant_details SET
                name = %(Name)s,
                Ownership = %(Ownership)s,
                Fuel_Type = %(Fuel_Type)s,
                Rated_Capacity = %(Rated_Capacity)s,
                PAF = %(PAF)s,
                PLF = %(PLF)s,
                Aux_Consumption = %(Aux_Consumption)s,
                Variable_Cost = %(Variable_Cost)s,
                Type = %(Type)s,
                Technical_Minimum = %(Technical_Minimum)s,
                Max_Power = %(Max_Power)s,
                Min_Power = %(Min_Power)s
            WHERE Code = %(Code)s
        """
        cursor.execute(update_query, data.dict())
        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "Plant data updated successfully"}

    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"MySQL Error: {err}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {e}")


@router.delete("/")
def delete_plant_data(code: str = Body(..., embed=True)):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        delete_query = "DELETE FROM plant_details WHERE Code = %s"
        cursor.execute(delete_query, (code,))
        conn.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="No record found with the given Code")

        cursor.close()
        conn.close()
        return {"message": "Plant data deleted successfully"}

    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"MySQL Error: {err}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {e}")

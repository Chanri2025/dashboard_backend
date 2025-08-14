from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
from pymongo import MongoClient
import mysql.connector
import os
from dotenv import load_dotenv
from utils.date_utils import parse_start_timestamp
from utils.mongo_helpers import to_float, convert_decimal128

# Load env
load_dotenv()
router = APIRouter()

# MySQL Config
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAMES').split(',')[1],
}

# MongoDB Config
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
db = client["powercasting"]


@router.get("/dashboard")
def get_dashboard_data(start: Optional[str] = Query(None), end: Optional[str] = Query(None)):
    try:
        match = {}
        if start:
            match.setdefault("TimeStamp", {})["$gte"] = parse_start_timestamp(start)
        if end:
            match.setdefault("TimeStamp", {})["$lte"] = parse_start_timestamp(end)

        # MySQL plant count
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT COUNT(TYPE) AS count FROM plant_details;")
        plant_count_row = cursor.fetchone()
        cursor.close()
        conn.close()
        plant_count = plant_count_row["count"] if plant_count_row else 0

        # MongoDB Demand Aggregation
        demand_pipeline = []
        if match:
            demand_pipeline.append({"$match": match})
        demand_pipeline.append({
            "$group": {
                "_id": None,
                "total_actual": {"$sum": "$Demand(Actual)"},
                "total_predicted": {"$sum": "$Demand(Pred)"}
            }
        })
        demand_res = list(db["Demand"].aggregate(demand_pipeline))
        total_actual = to_float(demand_res[0]["total_actual"]) if demand_res else 0.0
        total_predicted = to_float(demand_res[0]["total_predicted"]) if demand_res else 0.0

        # MongoDB Output Aggregation
        output_pipeline = []
        if match:
            output_pipeline.append({"$match": match})
        output_pipeline.append({
            "$group": {"_id": None, "avg_price": {"$avg": "$Cost_Per_Block"}}
        })
        avg_res = list(db["Demand_Output"].aggregate(output_pipeline))
        average_price = to_float(avg_res[0]["avg_price"]) if avg_res else 0.0

        return {
            "plant_count": plant_count,
            "demand_actual": round(total_actual, 3),
            "demand_predicted": round(total_predicted, 3),
            "avg_price": round(average_price, 2)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data")
def get_demand_data(start_date: str = Query(...), end_date: str = Query(...)):
    try:
        start_dt = parse_start_timestamp(start_date)
        end_dt = parse_start_timestamp(end_date)

        cursor = db["Demand"].find(
            {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}},
            {"_id": 0}
        ).sort("TimeStamp", 1)

        raw_docs = list(cursor)
        clean_docs = []
        for doc in raw_docs:
            doc = convert_decimal128(doc)
            ts = doc.get("TimeStamp")
            if isinstance(ts, datetime):
                doc["TimeStamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")
            clean_docs.append(doc)

        return {"demand": clean_docs}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from utils.mongo_helpers import convert_decimal128, format_timestamp, to_float
from Helpers.helpers import parse_start_timestamp

load_dotenv()
router = APIRouter()

# MongoDB setup
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
db = client["powercasting"]


@router.get("/all")
def get_price_data():
    """Return all price data from IEX_Price collection"""
    try:
        docs = list(db["IEX_Price"].find({}, {"_id": 0}))
        cleaned = [format_timestamp(convert_decimal128(doc)) for doc in docs]
        return cleaned
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/range")
def get_demand_range(
        start: str = Query(..., description="Start datetime in format 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD'"),
        end: str = Query(..., description="End datetime in same format")
):
    """Return predicted IEX prices between timestamps"""
    try:
        start_dt = parse_start_timestamp(start)
        end_dt = parse_start_timestamp(end)

        query = {"TimeStamp": {"$gte": start_dt, "$lte": end_dt}}
        projection = {"_id": 0, "TimeStamp": 1, "Pred_Price": 1}

        cursor = db["IEX_Generation"].find(query, projection).sort("TimeStamp", 1)

        rows = []
        for doc in cursor:
            rows.append({
                "TimeStamp": doc["TimeStamp"].strftime("%Y-%m-%d %H:%M:%S"),
                "predicted": to_float(doc.get("Pred_Price", 0))
            })

        total_predicted = sum(r["predicted"] for r in rows)
        avg_predicted = round(total_predicted / len(rows), 2) if rows else None

        return {
            "data": rows,
            "summary": {
                "total_predicted": total_predicted,
                "average_predicted": avg_predicted
            }
        }

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard")
def get_dashboard(
        start: Optional[str] = Query(None),
        end: Optional[str] = Query(None)
):
    """Return average actual and predicted price from IEX_Price collection"""
    try:
        match = {}
        if start:
            match.setdefault("TimeStamp", {})["$gte"] = parse_start_timestamp(start)
        if end:
            match.setdefault("TimeStamp", {})["$lte"] = parse_start_timestamp(end)

        pipeline = []
        if match:
            pipeline.append({"$match": match})
        pipeline.append({
            "$group": {
                "_id": None,
                "Avg_Price": {"$avg": "$Actual"},
                "Avg_Pred_Price": {"$avg": "$Pred"}
            }
        })

        result = list(db["IEX_Price"].aggregate(pipeline))

        if not result:
            return {"Avg_Price": 0, "Avg_Pred_Price": 0}

        avg_doc = result[0]
        return {
            "Avg_Price": round(to_float(avg_doc.get("Avg_Price")), 2),
            "Avg_Pred_Price": round(to_float(avg_doc.get("Avg_Pred_Price")), 2)
        }

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quantity")
def get_quantity_data(
        start: Optional[str] = Query(None),
        end: Optional[str] = Query(None)
):
    """Return IEX_Generation quantity data with optional filters"""
    try:
        match = {}
        if start:
            match.setdefault("TimeStamp", {})["$gte"] = parse_start_timestamp(start)
        if end:
            match.setdefault("TimeStamp", {})["$lte"] = parse_start_timestamp(end)

        raw_docs = list(db["IEX_Generation"].find(match, {"_id": 0}))
        clean_docs = [format_timestamp(convert_decimal128(doc)) for doc in raw_docs]

        return clean_docs

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid timestamp: {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

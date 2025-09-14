# utils/transaction_logger.py
from datetime import datetime, timedelta
from bson import ObjectId


def convert_bson(obj):
    """Helper to safely convert ObjectId/Datetime for JSON storage"""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def log_transaction_sync(db, log: dict):
    """Write a log entry into Transaction_History (synchronous client)"""
    coll = db["Transaction_History"]
    coll.insert_one(log)


def build_log(request, response_status, response_body, duration_ms: int):
    """Build log document"""
    return {
        "endpoint": request.url.path,
        "method": request.method,
        "query_params": dict(request.query_params),
        "headers": dict(request.headers),
        "body": getattr(request.state, "body", None),
        "response_status": response_status,
        "response_body": response_body,
        "author": request.headers.get("X-User-Email"),
        "timestamp": datetime.utcnow() + timedelta(hours=5, minutes=30),  # IST
        "duration_ms": duration_ms,
    }

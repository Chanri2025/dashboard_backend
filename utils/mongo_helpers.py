from bson.decimal128 import Decimal128
from datetime import datetime


def to_float(val):
    if isinstance(val, Decimal128):
        return float(val.to_decimal())
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def convert_decimal128(obj):
    if isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    if isinstance(obj, dict):
        return {k: convert_decimal128(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_decimal128(v) for v in obj]
    return obj


def format_timestamp(doc):
    ts = doc.get("TimeStamp")
    if isinstance(ts, datetime):
        doc["TimeStamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")
    return doc

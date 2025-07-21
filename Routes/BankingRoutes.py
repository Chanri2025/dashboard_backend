from datetime import datetime, time
from flask import Blueprint, jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os

bankingAPI = Blueprint('banking', __name__)
load_dotenv()

# Mongo config
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
client = MongoClient(mongo_uri)
db = client["powercasting"]


def normalize_date_str(s):
    if not s:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def parse_date(d):
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(d, fmt).date()
            except ValueError:
                continue
    return None


def parse_time(t):
    if isinstance(t, datetime):
        return t.time()
    if isinstance(t, str):
        for fmt in ('%H:%M:%S', '%H:%M'):
            try:
                return datetime.strptime(t, fmt).time()
            except ValueError:
                continue
    return None


def parse_timestamp_any(ts):
    """
    Accept either a datetime (return as‑is) or a 'Sun, 31 Mar 2024 23:45:00 GMT' string.
    """
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        # strip off any trailing ' GMT' if present
        s = ts.rstrip(" GMT")
        return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S")
    # fallback—treat as minimal date
    return datetime.min


@bankingAPI.route('/', methods=['GET'])
def get_consumer_data():
    # 1) pull params
    args = request.args or {}
    body = request.get_json(silent=True) or {}
    raw_start = args.get('start_date') or body.get('start_date')
    raw_end = args.get('end_date') or body.get('end_date')

    start_norm = normalize_date_str(raw_start)
    end_norm = normalize_date_str(raw_end)

    # 2) build date range query on Date
    query = {}
    if start_norm and end_norm:
        sd = datetime.strptime(start_norm, '%Y-%m-%d')
        ed = datetime.strptime(end_norm, '%Y-%m-%d')
        query["Date"] = {
            "$gte": datetime.combine(sd.date(), time.min),
            "$lte": datetime.combine(ed.date(), time.max),
        }

    # 3) fetch from Mongo (no sort here)
    cursor = db["Banking_Data"].find(query, {'_id': 0})
    docs = list(cursor)

    # 4) combine date+time into Start_DateTime / End_DateTime
    results = []
    for doc in docs:
        d_obj = parse_date(doc.get('Date'))
        st_obj = parse_time(doc.get('Start_Time'))
        et_obj = parse_time(doc.get('End_Time'))

        doc['Start_DateTime'] = (
            datetime.combine(d_obj, st_obj).strftime('%Y-%m-%d %H:%M:%S')
            if d_obj and st_obj else None
        )
        doc['End_DateTime'] = (
            datetime.combine(d_obj, et_obj).strftime('%Y-%m-%d %H:%M:%S')
            if d_obj and et_obj else None
        )
        results.append(doc)

    # 5) sort by TimeStamp (whether it's datetime or string)
    results.sort(key=lambda d: parse_timestamp_any(d.get("TimeStamp")))

    return jsonify(results), 200

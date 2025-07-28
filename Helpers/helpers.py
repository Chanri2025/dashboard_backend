# helpers.py
from datetime import datetime


def normalize_date_str(s):
    """
    Turn 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD' into 'YYYY-MM-DD'.
    """
    if not s:
        return None
    for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%d'):
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
    Accept either a datetime or an RFC‑2822 string like
    'Sun, 31 Mar 2024 23:45:00 GMT'.
    """
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        s = ts.rstrip(" GMT")
        try:
            return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S")
        except ValueError:
            pass
    return datetime.min


def parse_iso_timestamp(s: str):
    """
    Parse ISO8601 strings like '2023-04-01T00:00:00Z'
    or '2023-04-01T00:00:00+00:00' into a datetime.
    """
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None

def parse_start_timestamp(raw_start: str) -> datetime:
    """
    Normalize 'YYYY-MM-DD HH:MM' or 'YYYY-MM-DD' → datetime(timestamp).
    Raises ValueError on bad format.
    """
    if not raw_start:
        raise ValueError("Missing 'start_date'")
    date_only = normalize_date_str(raw_start)
    if not date_only:
        raise ValueError(f"Invalid date format: {raw_start}")
    try:
        return datetime.strptime(raw_start, "%Y-%m-%d %H:%M")
    except ValueError:
        # allow just date (defaults to midnight)
        return datetime.strptime(date_only, "%Y-%m-%d")
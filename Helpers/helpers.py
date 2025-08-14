from datetime import datetime
from typing import Union, Optional


def normalize_date_str(s: str) -> Optional[str]:
    if not s:
        return None
    for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def parse_date(d: Union[str, datetime]) -> Optional[datetime.date]:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(d, fmt).date()
            except ValueError:
                continue
    return None


def parse_time(t: Union[str, datetime]) -> Optional[datetime.time]:
    if isinstance(t, datetime):
        return t.time()
    if isinstance(t, str):
        for fmt in ('%H:%M:%S', '%H:%M'):
            try:
                return datetime.strptime(t, fmt).time()
            except ValueError:
                continue
    return None


def parse_timestamp_any(ts: Union[str, datetime]) -> datetime:
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, str):
        s = ts.rstrip(" GMT")
        try:
            return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S")
        except ValueError:
            pass
    return datetime.min


def parse_iso_timestamp(s: str) -> Optional[datetime]:
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def parse_start_timestamp(raw_start: str) -> datetime:
    if not raw_start:
        raise ValueError("Missing 'start_date'")
    date_only = normalize_date_str(raw_start)
    if not date_only:
        raise ValueError(f"Invalid date format: {raw_start}")
    try:
        return datetime.strptime(raw_start, "%Y-%m-%d %H:%M")
    except ValueError:
        return datetime.strptime(date_only, "%Y-%m-%d")

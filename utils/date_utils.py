from datetime import datetime, date, time
from typing import Union, Optional


def normalize_date_str(s: str) -> Optional[str]:
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_date(d: Union[str, datetime]) -> Optional[date]:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(d, fmt).date()
            except ValueError:
                continue
    return None


def parse_time(t: Union[str, datetime]) -> Optional[time]:
    if isinstance(t, datetime):
        return t.time()
    if isinstance(t, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
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
    """
    Accepts 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', or 'YYYY-MM-DD HH:MM:SS'
    """
    if not raw_start:
        raise ValueError("Missing 'start_date'")

    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")

    for fmt in formats:
        try:
            return datetime.strptime(raw_start, fmt)
        except ValueError:
            continue

    raise ValueError(f"Invalid date format: {raw_start}")


def parse_end_timestamp(raw_end: str) -> datetime:
    """
    Accepts 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM', or 'YYYY-MM-DD HH:MM:SS'
    If only a date is provided, returns end-of-day (23:59:59.999999).
    """
    if not raw_end:
        raise ValueError("Missing 'end_date'")

    fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")
    for fmt in fmts:
        try:
            dt = datetime.strptime(raw_end, fmt)
            # If format was only date, push to end-of-day
            if fmt == "%Y-%m-%d":
                return dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            return dt
        except ValueError:
            continue

    raise ValueError(f"Invalid date format: {raw_end}")

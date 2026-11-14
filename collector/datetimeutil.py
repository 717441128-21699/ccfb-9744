"""datetime 小工具。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utc_plus(**kwargs) -> datetime:
    return datetime.now(timezone.utc) + timedelta(**kwargs)


def parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

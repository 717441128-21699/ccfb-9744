"""仿真站内部工具：真值读取、本地时间渲染、限流桶。"""
from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from collector import canonical, config
from collector.carriers import CARRIERS

# 各家页面时间格式（刻意各不相同）
TIME_FORMATS = {
    "alpha": "%Y-%m-%d %H:%M:%S",
    "beta": "%d/%m/%Y %H:%M",
    "gamma": "%Y-%m-%dT%H:%M:%S",
    "delta": "%d-%m-%Y %H:%M",
    "epsilon": "%Y/%m/%d %H:%M",
    "zeta": "%d.%m.%Y %H:%M",
    "eta": "%Y-%m-%d %H:%M",
    "theta": "%m/%d/%Y %I:%M %p",
}


def sim_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(config.SIM_DB_PATH), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def get_runtime(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM sim_runtime WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_runtime(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO sim_runtime(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value),
    )
    conn.commit()


def visible_events(conn: sqlite3.Connection, tracking_no: str) -> list[sqlite3.Row]:
    """只返回已到可见时间且未撤回的节点——这就是站点此刻对外展示的轨迹。"""
    now_iso = canonical.to_utc_iso(canonical.now_utc())
    return conn.execute(
        "SELECT * FROM sim_events WHERE tracking_no=? AND visible_at<=? AND retracted=0 "
        "ORDER BY seq", (tracking_no, now_iso),
    ).fetchall()


def waybill_exists(conn: sqlite3.Connection, tracking_no: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sim_waybills WHERE tracking_no=?", (tracking_no,)
    ).fetchone() is not None


def render_local(carrier: str, utc_iso: str) -> str:
    """把真值 UTC 时间按承运商所在时区/格式渲染成本地字符串。"""
    dt = datetime.fromisoformat(utc_iso).astimezone(
        ZoneInfo(canonical.CARRIER_TIMEZONES[carrier]))
    return dt.strftime(TIME_FORMATS[carrier])


class RateLimiter:
    """每承运商一个简单令牌桶；超限返回建议等待秒数。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: dict[str, tuple[float, float]] = {}  # code -> (tokens, last_ts)

    def allow(self, carrier: str) -> tuple[bool, float]:
        rps = CARRIERS[carrier].rate_rps
        with self._lock:
            now = time.monotonic()
            tokens, last = self._state.get(carrier, (rps, now))
            tokens = min(rps, tokens + (now - last) * rps)
            if tokens >= 1.0:
                self._state[carrier] = (tokens - 1.0, now)
                return True, 0.0
            wait = (1.0 - tokens) / rps
            self._state[carrier] = (tokens, now)
            return False, wait


RATE_LIMITER = RateLimiter()

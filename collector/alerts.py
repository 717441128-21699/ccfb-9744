"""告警服务：同类告警去重 + 冷却，解决后可重新触发。线程安全。"""
from __future__ import annotations

import sqlite3

from . import config
from .canonical import now_utc, to_utc_iso
from .db import execute_with_retry

SEVERITY_BY_KIND = {
    "parse_error": "critical",
    "schema_changed": "critical",
    "site_down": "critical",
    "login_failed": "critical",
    "unknown_status": "warning",
    "rate_limited": "warning",
    "unknown_waybill": "info",
}


def raise_alert(conn: sqlite3.Connection, kind: str, message: str, *,
                carrier_code: str | None = None, waybill_id: int | None = None,
                cooldown_seconds: int | None = None) -> bool:
    """写入告警。冷却期内的同类告警被折叠；返回是否真正写入。"""
    cooldown = config.ALERT_COOLDOWN_SECONDS if cooldown_seconds is None else cooldown_seconds
    dedup_key = f"{kind}|{carrier_code or '*'}|{waybill_id or '*'}"
    now = to_utc_iso(now_utc())
    row = conn.execute(
        "SELECT id, created_at, resolved_at FROM alerts WHERE dedup_key=?",
        (dedup_key,),
    ).fetchone()
    if row is not None:
        if row["resolved_at"] is None:
            # 未解决：冷却期内不重复写
            from datetime import datetime
            created = datetime.fromisoformat(row["created_at"])
            if (now_utc() - created).total_seconds() < cooldown:
                return False
            # 超过冷却仍未恢复：更新时间戳保持热度，不新增行
            execute_with_retry(
                conn, "UPDATE alerts SET created_at=?, message=? WHERE id=?",
                (now, message, row["id"]))
            return False
        # 已解决后再次发生：新建一条
    for _ in range(5):
        try:
            execute_with_retry(
                conn,
                "INSERT INTO alerts (created_at,carrier_code,waybill_id,kind,severity,"
                "message,dedup_key) VALUES (?,?,?,?,?,?,?)",
                (now, carrier_code, waybill_id, kind,
                 SEVERITY_BY_KIND.get(kind, "warning"), message, dedup_key),
            )
            return True
        except sqlite3.IntegrityError:
            # 并发分片同时插入同一 dedup_key：重读并按去重/冷却规则处理
            existing = conn.execute(
                "SELECT id, resolved_at FROM alerts WHERE dedup_key=?",
                (dedup_key,)).fetchone()
            if existing is not None:
                return False
    return False
    return True


def resolve_alert(conn: sqlite3.Connection, kind: str, *,
                  carrier_code: str | None = None, waybill_id: int | None = None) -> None:
    dedup_key = f"{kind}|{carrier_code or '*'}|{waybill_id or '*'}"
    execute_with_retry(
        conn,
        "UPDATE alerts SET resolved_at=? WHERE dedup_key=? AND resolved_at IS NULL",
        (to_utc_iso(now_utc()), dedup_key),
    )

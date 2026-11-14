"""轨迹合并入库：增量追加、幂等去重、节点撤回、变更流水。"""
from __future__ import annotations

import sqlite3

from .canonical import CanonicalNode, Status, TERMINAL_STATUSES, now_utc, to_utc_iso
from .db import execute_with_retry


class MergeResult:
    def __init__(self) -> None:
        self.added = 0
        self.duplicates = 0
        self.retracted = 0
        self.readded = 0
        self.unknown_statuses: list[str] = []
        self.new_status: str | None = None


def merge_nodes(conn: sqlite3.Connection, waybill_id: int, carrier: str,
                nodes: list[CanonicalNode], *, snapshot_mode: bool) -> MergeResult:
    """把本次抓取到的规范节点与库内现有节点合并。

    - 新 uid：INSERT（UNIQUE(waybill_id,uid) 兜底，并发也不会重复）；
    - 已存在且有效：跳过，只计 duplicates；
    - 已撤回但重新出现：撤销撤回标记；
    - snapshot_mode（eta）：库内有、本次快照没有的有效节点标记撤回，保留审计痕迹。
    """
    result = MergeResult()
    now = to_utc_iso(now_utc())
    if not nodes:
        # 空列表绝不该走到这里（解析器会先抛 ParseError），双保险
        return result

    seen_uids: set[str] = set()
    latest_status = Status.PENDING.value
    latest_seq = -1

    for node in nodes:
        seen_uids.add(node.uid)
        if node.status == Status.UNKNOWN.value:
            result.unknown_statuses.append(node.uid)
        existing = conn.execute(
            "SELECT id, retracted FROM tracking_nodes WHERE waybill_id=? AND uid=?",
            (waybill_id, node.uid),
        ).fetchone()
        if existing is None:
            execute_with_retry(
                conn,
                "INSERT INTO tracking_nodes (waybill_id,uid,seq,event_time_utc,status,"
                "description,location,retracted,first_seen_at) VALUES (?,?,?,?,?,?,?,0,?)",
                (waybill_id, node.uid, node.seq, node.event_time_utc, node.status,
                 node.description, node.location, now))
            execute_with_retry(
                conn,
                "INSERT INTO node_changes (waybill_id,change_type,node_uid,from_status,"
                "to_status,changed_at) VALUES (?,?,?,?,?,?)",
                (waybill_id, "added", node.uid, None, node.status, now))
            result.added += 1
        elif existing["retracted"]:
            execute_with_retry(
                conn,
                "UPDATE tracking_nodes SET retracted=0, retracted_at=NULL WHERE id=?",
                (existing["id"],))
            execute_with_retry(
                conn,
                "INSERT INTO node_changes (waybill_id,change_type,node_uid,from_status,"
                "to_status,changed_at) VALUES (?,?,?,?,?,?)",
                (waybill_id, "added", node.uid, None, node.status, now))
            result.readded += 1
        else:
            result.duplicates += 1

        if node.seq > latest_seq:
            latest_seq = node.seq
            latest_status = node.status

    if snapshot_mode:
        active_rows = conn.execute(
            "SELECT id, uid FROM tracking_nodes WHERE waybill_id=? AND retracted=0",
            (waybill_id,),
        ).fetchall()
        for row in active_rows:
            if row["uid"] not in seen_uids:
                execute_with_retry(
                    conn,
                    "UPDATE tracking_nodes SET retracted=1, retracted_at=? WHERE id=?",
                    (now, row["id"]))
                execute_with_retry(
                    conn,
                    "INSERT INTO node_changes (waybill_id,change_type,node_uid,from_status,"
                    "to_status,changed_at) VALUES (?,?,?,?,?,?)",
                    (waybill_id, "retracted", row["uid"], None, None, now))
                result.retracted += 1

    # 运单当前状态 = 最大 seq 节点的状态；终态则归档
    prev = conn.execute(
        "SELECT current_status, active FROM waybills WHERE id=?", (waybill_id,)
    ).fetchone()
    if prev and prev["current_status"] != latest_status:
        execute_with_retry(
            conn,
            "INSERT INTO node_changes (waybill_id,change_type,node_uid,from_status,"
            "to_status,changed_at) VALUES (?,?,?,?,?,?)",
            (waybill_id, "status_advanced", None,
             prev["current_status"], latest_status, now))
    result.new_status = latest_status
    # 终态归档时机交给 compute_next_poll 结合宽限复核次数决定，
    # 这里只更新当前状态，避免一签收就再也无法感知事后撤回
    execute_with_retry(
        conn,
        "UPDATE waybills SET current_status=? WHERE id=?",
        (latest_status, waybill_id))
    return result


def compute_next_poll(conn: sqlite3.Connection, waybill_id: int,
                      poll_overrides: dict[str, int], *, failed: bool,
                      failure_count: int) -> str:
    """自适应下一次查询时间，并维护终态宽限复核与归档标志。

    - 成功在途：按当前状态对应的间隔（节点更新越快的状态查得越勤）；
    - 终态：进入宽限复核期，仍按 TERMINAL_GRACE_INTERVAL 复核 N 次，
      以捕获事后撤回/迟到节点；复核用尽才归档（active=0）并停止调度；
    - 失败：指数退避，但封顶。
    """
    row = conn.execute(
        "SELECT current_status, active, terminal_polls FROM waybills WHERE id=?",
        (waybill_id,)).fetchone()
    from .datetimeutil import utc_plus
    from . import config

    if failed:
        backoff = min(config.MAX_REMINDER_BACKOFF,
                      config.FAST_POLL_SECONDS * (2 ** min(failure_count, 6)))
        return to_utc_iso(utc_plus(seconds=backoff))
    if row is None:
        return to_utc_iso(utc_plus(days=3650))

    status = row["current_status"]
    if status in TERMINAL_STATUSES:
        polls = row["terminal_polls"]
        if polls + 1 > config.TERMINAL_GRACE_POLLS:
            # 宽限复核已用尽：归档，停止调度
            execute_with_retry(
                conn,
                "UPDATE waybills SET active=0 WHERE id=?",
                (waybill_id,))
            return to_utc_iso(utc_plus(days=3650))
        # 仍在宽限复核期：保持调度并累计复核次数
        execute_with_retry(
            conn,
            "UPDATE waybills SET active=1, terminal_polls=terminal_polls+1 WHERE id=?",
            (waybill_id,))
        return to_utc_iso(utc_plus(seconds=config.TERMINAL_GRACE_INTERVAL))

    if not row["active"]:
        return to_utc_iso(utc_plus(days=3650))
    interval = poll_overrides.get(status, config.POLL_INTERVALS_SECONDS.get(
        status, config.POLL_INTERVALS_SECONDS["UNKNOWN"]))
    return to_utc_iso(utc_plus(seconds=interval))

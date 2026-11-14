"""运单批量导入：按单号规则自动识别承运商，识别不了的单列。"""
from __future__ import annotations

import csv
import io
import sqlite3
import uuid

from .canonical import now_utc, to_utc_iso
from .carriers import recognize
from .config import FAST_POLL_SECONDS
from .db import execute_with_retry
from .datetimeutil import utc_plus


def import_waybills(conn: sqlite3.Connection, raw_text: str, filename: str = "paste") -> dict:
    """导入单号（每行一个，或 CSV 首列）。返回批次统计。"""
    numbers: list[str] = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        # 兼容 CSV：取第一列
        first = next(csv.reader(io.StringIO(line)))[0].strip()
        if first:
            numbers.append(first)

    batch_id = uuid.uuid4().hex[:12]
    now = to_utc_iso(now_utc())
    first_due = to_utc_iso(utc_plus(seconds=FAST_POLL_SECONDS))
    recognized = unrecognized = 0
    dup = 0

    for no in numbers:
        carrier = recognize(no)
        if carrier is None:
            try:
                execute_with_retry(
                    conn,
                    "INSERT INTO unrecognized_waybills (tracking_no,import_batch,reason,created_at) "
                    "VALUES (?,?,?,?)",
                    (no, batch_id, "no carrier pattern matched", now))
            except sqlite3.IntegrityError:
                dup += 1
                continue
            unrecognized += 1
            continue
        try:
            execute_with_retry(
                conn,
                "INSERT INTO waybills (tracking_no,carrier_code,first_seen_at,next_poll_at,"
                "import_batch) VALUES (?,?,?,?,?)",
                (no, carrier, now, first_due, batch_id))
            recognized += 1
        except sqlite3.IntegrityError:
            dup += 1

    execute_with_retry(
        conn,
        "INSERT INTO import_batches (id,filename,total,recognized,unrecognized,created_at) "
        "VALUES (?,?,?,?,?,?)",
        (batch_id, filename, len(numbers), recognized, unrecognized, now))
    return {
        "batch_id": batch_id, "total": len(numbers),
        "recognized": recognized, "unrecognized": unrecognized,
        "duplicates": dup,
    }

"""SQLite 持久层。

设计要点：
- WAL + busy_timeout，worker 线程与 Web 控制台可并发读写；
- 所有写操作走短事务，函数自带 sqlite locked 重试；
- waybill_leases 视图式租约：worker 领取运单时 CAS 更新 owner/lease_expires_at，
  进程被杀后租约自然过期，重启可继续，不漏单；
- tracking_nodes 以 (waybill_id, uid) 建唯一索引保证同一节点绝不重复入库；
  撤回通过 retracted=1 表达并保留审计痕迹，不物理删除。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path

from . import config

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS carrier_configs (
    code            TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    parser_version  TEXT NOT NULL DEFAULT 'v1',
    base_url        TEXT NOT NULL,
    mode            TEXT NOT NULL,                 -- browser_page|browser_json|request_json|request_signed
    needs_login     INTEGER NOT NULL DEFAULT 0,
    login_user      TEXT,
    login_pass      TEXT,
    rate_rps        REAL NOT NULL DEFAULT 2.0,
    poll_overrides  TEXT NOT NULL DEFAULT '{}',    -- JSON: 状态->间隔秒，覆盖默认
    params_json     TEXT NOT NULL DEFAULT '{}',    -- 签名密钥等其他参数
    circuit_open_until TEXT,                       -- 熔断打开到何时（UTC ISO）
    consecutive_fails INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS waybills (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    tracking_no      TEXT NOT NULL UNIQUE,
    carrier_code     TEXT,                         -- 识别不出来为 NULL
    current_status   TEXT NOT NULL DEFAULT 'PENDING',
    first_seen_at    TEXT NOT NULL,
    last_polled_at   TEXT,
    next_poll_at     TEXT NOT NULL,
    last_success_at  TEXT,
    poll_count       INTEGER NOT NULL DEFAULT 0,
    active           INTEGER NOT NULL DEFAULT 1,   -- 0=终态归档，不再调度
    terminal_polls   INTEGER NOT NULL DEFAULT 0,   -- 进入终态后的宽限复核次数
    lease_owner      TEXT,                         -- 领取该单的 worker 实例
    lease_expires_at TEXT,                         -- 租约过期时间；崩溃后可被重新领取
    fail_count       INTEGER NOT NULL DEFAULT 0,
    import_batch     TEXT,
    FOREIGN KEY(carrier_code) REFERENCES carrier_configs(code)
);
CREATE INDEX IF NOT EXISTS idx_waybills_due
    ON waybills(carrier_code, next_poll_at) WHERE active = 1;

CREATE TABLE IF NOT EXISTS tracking_nodes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    waybill_id      INTEGER NOT NULL,
    uid             TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    event_time_utc  TEXT NOT NULL,
    status          TEXT NOT NULL,
    description     TEXT NOT NULL,
    location        TEXT NOT NULL DEFAULT '',
    retracted       INTEGER NOT NULL DEFAULT 0,
    first_seen_at   TEXT NOT NULL,
    retracted_at    TEXT,
    UNIQUE(waybill_id, uid),
    FOREIGN KEY(waybill_id) REFERENCES waybills(id)
);
CREATE INDEX IF NOT EXISTS idx_nodes_waybill ON tracking_nodes(waybill_id, seq);

CREATE TABLE IF NOT EXISTS fetch_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    waybill_id      INTEGER NOT NULL,
    carrier_code    TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT NOT NULL,
    ok              INTEGER NOT NULL,
    http_status     INTEGER,
    nodes_seen      INTEGER NOT NULL DEFAULT 0,
    nodes_added     INTEGER NOT NULL DEFAULT 0,
    nodes_retracted INTEGER NOT NULL DEFAULT 0,
    error_kind      TEXT,                          -- parse_error|rate_limited|http_error|timeout|unknown_waybill|login_failed
    error_message   TEXT,
    attempt_no      INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_fetch_waybill ON fetch_logs(waybill_id, id);

CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT NOT NULL,
    carrier_code    TEXT,
    waybill_id      INTEGER,
    kind            TEXT NOT NULL,                 -- parse_error|site_down|login_failed|rate_limited|unknown_status|schema_changed|unknown_waybill
    severity        TEXT NOT NULL,                 -- critical|warning|info
    message         TEXT NOT NULL,
    dedup_key       TEXT NOT NULL UNIQUE,
    resolved_at     TEXT
);

CREATE TABLE IF NOT EXISTS import_batches (
    id              TEXT PRIMARY KEY,
    filename        TEXT NOT NULL,
    total           INTEGER NOT NULL,
    recognized      INTEGER NOT NULL,
    unrecognized    INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS unrecognized_waybills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tracking_no     TEXT NOT NULL UNIQUE,
    import_batch    TEXT,
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS node_changes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    waybill_id      INTEGER NOT NULL,
    change_type     TEXT NOT NULL,                 -- added|retracted|status_advanced
    node_uid        TEXT,
    from_status     TEXT,
    to_status       TEXT,
    changed_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_changes_time ON node_changes(changed_at);

CREATE TABLE IF NOT EXISTS scheduler_state (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL
);
"""

_init_lock = threading.Lock()
_initialized = False


def get_conn(db_path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path or config.DB_PATH),
        timeout=30,
        isolation_level=None,  # 显式事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path | None = None) -> None:
    global _initialized
    with _init_lock:
        conn = get_conn(db_path)
        try:
            conn.executescript(_SCHEMA)
        finally:
            conn.close()
        _initialized = True


@contextmanager
def transaction(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def execute_with_retry(conn: sqlite3.Connection, sql: str, params: Iterable = (), *,
                       retries: int = 50, delay: float = 0.1):
    """SQLITE_BUSY/LOCKED 时线性退避重试。"""
    last_err = None
    for i in range(retries):
        try:
            cur = conn.execute(sql, tuple(params))
            return cur
        except sqlite3.OperationalError as e:
            last_err = e
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(delay * (i + 1))
                continue
            raise
    raise last_err  # type: ignore[misc]

"""把承运商注册表落库到 carrier_configs（已存在的保留 Web 端修改，仅补全新增字段）。"""
from __future__ import annotations

import json
import sqlite3

from . import config
from .canonical import now_utc, to_utc_iso
from .carriers import CARRIERS
from .db import execute_with_retry


def bootstrap_carriers(conn: sqlite3.Connection, base_url: str) -> None:
    now = to_utc_iso(now_utc())
    for code, spec in CARRIERS.items():
        row = conn.execute(
            "SELECT code FROM carrier_configs WHERE code=?", (code,)
        ).fetchone()
        if row:
            continue
        execute_with_retry(
            conn,
            "INSERT INTO carrier_configs (code,name,enabled,parser_version,base_url,"
            "mode,needs_login,login_user,login_pass,rate_rps,poll_overrides,params_json,"
            "updated_at) VALUES (?,?,1,'v1',?,?,?,?,?,?,?,?,?)",
            (code, spec.name, f"{base_url}/{code}", spec.mode,
             1 if spec.needs_login else 0, spec.login_user, spec.login_pass,
             spec.rate_rps, json.dumps({}), json.dumps(spec.params), now),
        )

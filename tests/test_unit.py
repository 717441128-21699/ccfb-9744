"""纯单元测试：单号识别、状态映射、时区换算、幂等合并、撤回、调度间隔。"""
from __future__ import annotations

from collector.canonical import (CARRIER_STATUS_MAP, CARRIER_TIMEZONES,
                                 CanonicalNode, Status, map_status, parse_local,
                                 to_utc_iso)
from collector.carriers import recognize


def test_recognize_all_eight_carriers():
    samples = {
        "alpha": "YP2600000001",
        "beta": "UK700000001",
        "gamma": "GMA-80000001",
        "delta": "BR31000000001BR",
        "epsilon": "JP450000000001JP",
        "zeta": "Z9000000001EU",
        "eta": "EE000000001",
        "theta": "TH600000000001",
    }
    for expect, no in samples.items():
        assert recognize(no) == expect, no


def test_recognize_rejects_unknown():
    for bad in ["BAD-0001", "WRONG99999X", "YP_SHORT", "xxYP2600000001"]:
        assert recognize(bad) is None, bad


def test_status_mapping_covers_every_lifecycle_raw():
    from sim.generate import LIFECYCLES
    for carrier, chain in LIFECYCLES.items():
        for raw in chain:
            assert map_status(carrier, raw) is not Status.UNKNOWN, (carrier, raw)


def test_unknown_raw_status_maps_to_unknown_not_crash():
    assert map_status("alpha", "SOME_NEW_CODE") is Status.UNKNOWN


def test_timezone_roundtrip_eight_zones():
    # 同一个 UTC 瞬间，在各承运商时区渲染再解析回来必须相等
    from datetime import datetime, timezone
    from sim.simlib import TIME_FORMATS, render_local
    instant = datetime(2026, 11, 14, 10, 30, tzinfo=timezone.utc)
    iso = to_utc_iso(instant)
    for carrier in CARRIER_TIMEZONES:
        local_text = render_local(carrier, iso)
        back = parse_local(carrier, local_text, TIME_FORMATS[carrier])
        assert to_utc_iso(back) == iso, carrier


def test_merge_is_idempotent_and_handles_retract(env):
    import uuid
    from collector.db import get_conn, init_db
    from collector.repository import merge_nodes
    init_db(env["config"].DB_PATH)
    conn = get_conn(env["config"].DB_PATH)
    unique_no = "UNIT-" + uuid.uuid4().hex[:10]
    conn.execute(
        "INSERT INTO waybills (tracking_no,carrier_code,first_seen_at,next_poll_at) "
        "VALUES (?,'alpha','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00')",
        (unique_no,))
    wb = conn.execute(
        "SELECT id FROM waybills WHERE tracking_no=?", (unique_no,)).fetchone()
    nodes = [
        CanonicalNode("alpha-01", 1, "2026-11-14T01:00:00+00:00",
                      "PENDING", "n1", "深圳"),
        CanonicalNode("alpha-02", 2, "2026-11-14T02:00:00+00:00",
                      "IN_TRANSIT", "n2", "香港"),
    ]
    r1 = merge_nodes(conn, wb["id"], "alpha", nodes, snapshot_mode=False)
    r2 = merge_nodes(conn, wb["id"], "alpha", nodes, snapshot_mode=False)
    assert r1.added == 2 and r2.added == 0 and r2.duplicates == 2
    assert conn.execute(
        "SELECT COUNT(*) c FROM tracking_nodes WHERE waybill_id=?",
        (wb["id"],)).fetchone()["c"] == 2

    # eta 式快照撤回：新快照里没有 alpha-02
    r3 = merge_nodes(conn, wb["id"], "alpha", nodes[:1], snapshot_mode=True)
    assert r3.retracted == 1
    row = conn.execute(
        "SELECT retracted FROM tracking_nodes WHERE uid='alpha-02' AND waybill_id=?",
        (wb["id"],)).fetchone()
    assert row["retracted"] == 1

    # 撤回节点重新出现：撤销标记而不是新增
    r4 = merge_nodes(conn, wb["id"], "alpha", nodes, snapshot_mode=True)
    assert r4.readded == 1
    row = conn.execute(
        "SELECT retracted FROM tracking_nodes WHERE uid='alpha-02' AND waybill_id=?",
        (wb["id"],)).fetchone()
    assert row["retracted"] == 0
    conn.close()

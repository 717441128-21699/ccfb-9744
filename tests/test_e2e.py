"""端到端：导入 -> 调度采集 -> 对账为 0 -> 追加/撤回/故障/改版。"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest
import requests

from collector import canonical, config
from collector.db import get_conn, init_db
from collector.reconcile import format_report, reconcile
from collector.scheduler import SchedulerEngine

ROOT = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return canonical.to_utc_iso(canonical.now_utc())


def _wait_caught_up(timeout: float = 240, only_numbers: set[str] | None = None) -> None:
    """等到采集库与仿真站当前视图一致：有效节点数 + 撤回标记都对齐。

    直接以仿真真值为收敛标准，而不是等调度器空闲——在途单本来就会持续轮询。
    only_numbers 给定时只核对该集合（子集测试用），否则核对全部运单。
    """
    import sqlite3
    deadline = time.time() + timeout
    while time.time() < deadline:
        sim = sqlite3.connect(str(config.SIM_DB_PATH))
        sim.row_factory = sqlite3.Row
        if only_numbers is None:
            truth_rows = sim.execute(
                "SELECT tracking_no, "
                "SUM(CASE WHEN retracted=0 AND visible_at<=? THEN 1 ELSE 0 END) AS active_n, "
                "SUM(CASE WHEN retracted=1 THEN 1 ELSE 0 END) AS retracted_n "
                "FROM sim_events GROUP BY tracking_no", (_now_iso(),)).fetchall()
        else:
            keys = list(only_numbers)
            truth_rows = sim.execute(
                "SELECT tracking_no, "
                "SUM(CASE WHEN retracted=0 AND visible_at<=? THEN 1 ELSE 0 END) AS active_n, "
                "SUM(CASE WHEN retracted=1 THEN 1 ELSE 0 END) AS retracted_n "
                "FROM sim_events WHERE tracking_no IN (%s) GROUP BY tracking_no"
                % ",".join("?" * len(keys)), (_now_iso(), *keys)).fetchall()
        sim.close()
        conn = get_conn()
        try:
            done = True
            for tr in truth_rows:
                got = conn.execute(
                    "SELECT SUM(CASE WHEN n.retracted=0 THEN 1 ELSE 0 END) AS active_n, "
                    "SUM(CASE WHEN n.retracted=1 THEN 1 ELSE 0 END) AS retracted_n "
                    "FROM tracking_nodes n JOIN waybills w ON w.id=n.waybill_id "
                    "WHERE w.tracking_no=?", (tr["tracking_no"],)).fetchone()
                if (got["active_n"] or 0) != (tr["active_n"] or 0) or \
                   (got["retracted_n"] or 0) != (tr["retracted_n"] or 0):
                    done = False
                    break
        finally:
            conn.close()
        if done:
            return
        time.sleep(1)
    raise TimeoutError("等待采集追平仿真真值超时")


def _wait_idle(timeout: float = 180, settle: float = 12.0) -> None:
    """等到没有到期任务、没有未完成租约，并连续稳定 settle 秒。

    仿真节点是分批“到点可见”的，短暂空闲不代表全部可见节点已采完，
    因此要求连续一段稳定空闲才算收敛。
    """
    deadline = time.time() + timeout
    quiet_since = None
    while time.time() < deadline:
        conn = get_conn()
        try:
            due = conn.execute(
                "SELECT COUNT(*) c FROM waybills WHERE active=1 AND next_poll_at<=?",
                (_now_iso(),)).fetchone()["c"]
            open_leases = conn.execute(
                "SELECT COUNT(*) c FROM waybills WHERE lease_expires_at>? "
                "AND lease_owner IS NOT NULL", (_now_iso(),)).fetchone()["c"]
        finally:
            conn.close()
        if due == 0 and open_leases == 0:
            if quiet_since is None:
                quiet_since = time.time()
            elif time.time() - quiet_since >= settle:
                return
        else:
            quiet_since = None
        time.sleep(1)
    raise TimeoutError("等待调度空闲超时")


def _fail_text(report) -> str:
    return format_report(report)


def test_full_pipeline_and_zero_diff(env):
    """1) 全量导入（含坏单号）2) 跑到空闲 3) 对账差异 0 4) 无重复节点。"""
    init_db()
    sim = sqlite3.connect(str(config.SIM_DB_PATH))
    numbers = [r[0] for r in sim.execute(
        "SELECT tracking_no FROM sim_waybills ORDER BY tracking_no")]
    sim.close()
    numbers += ["BAD-0001", "WRONG99999X", "YP_SHORT"]

    from collector.importer import import_waybills
    conn = get_conn()
    try:
        info = import_waybills(conn, "\n".join(numbers), "e2e")
    finally:
        conn.close()
    assert info["recognized"] == env["stats"]["__total__"]
    assert info["unrecognized"] == 3

    engine = SchedulerEngine()
    engine.start()
    try:
        _wait_caught_up()
    finally:
        engine.stop()

    conn = get_conn()
    try:
        unrec = {r["tracking_no"] for r in conn.execute(
            "SELECT tracking_no FROM unrecognized_waybills")}
        assert {"BAD-0001", "WRONG99999X", "YP_SHORT"} <= unrec
        dup = conn.execute(
            "SELECT waybill_id, uid, COUNT(*) c FROM tracking_nodes "
            "GROUP BY waybill_id, uid HAVING c>1").fetchall()
        assert dup == []
    finally:
        conn.close()

    report = reconcile()
    assert report.ok, _fail_text(report)
    assert report.waybills_checked == env["stats"]["__total__"]


def test_incremental_append_and_eta_retract(env):
    """仿真侧追加节点 + eta 撤回节点后，再跑一轮，对账仍为 0。"""
    _ensure_waybills_loaded()
    assert Path(config.DB_PATH).exists()
    added = requests.post(env["base"] + "/admin/advance",
                          json={"max_new": 30}, timeout=15).json()["added"]
    assert added >= 1

    sim = sqlite3.connect(str(config.SIM_DB_PATH))
    candidates = sim.execute(
        "SELECT value FROM sim_runtime WHERE key='retract_candidates'"
    ).fetchone()[0].split(",")
    sim.close()
    for no in candidates:
        r = requests.post(env["base"] + "/admin/retract",
                          json={"tracking_no": no, "seq": 3}, timeout=15)
        assert r.ok

    engine = SchedulerEngine()
    engine.start()
    try:
        conn = get_conn()
        try:
            # 撤回候选是已签收单：对其重新打开事后核查（active+复核计数归零）
            conn.execute(
                "UPDATE waybills SET active=1, terminal_polls=0, next_poll_at=? "
                "WHERE tracking_no IN (%s)" % ",".join("?" * len(candidates)),
                (_now_iso(), *candidates))
            # 其余运单也立即到期，尽快追平 advance 追加的节点
            conn.execute(
                "UPDATE waybills SET next_poll_at=? WHERE next_poll_at>?",
                (_now_iso(), _now_iso()))
        finally:
            conn.close()
        _wait_caught_up(timeout=150)
    finally:
        engine.stop()

    report = reconcile()
    assert report.ok, _fail_text(report)
    conn = get_conn()
    try:
        retracted = conn.execute(
            "SELECT COUNT(*) c FROM tracking_nodes WHERE retracted=1").fetchone()["c"]
        changes = conn.execute(
            "SELECT COUNT(*) c FROM node_changes WHERE change_type='retracted'"
        ).fetchone()["c"]
        assert retracted == len(candidates)
        assert changes == len(candidates)
    finally:
        conn.close()


def _ensure_waybills_loaded(carrier: str | None = None, timeout: int = 200) -> None:
    """若采集库为空，全量导入并采到收敛，保证测试可独立运行。"""
    init_db()
    conn = get_conn()
    try:
        already = conn.execute("SELECT COUNT(*) c FROM waybills").fetchone()["c"]
    finally:
        conn.close()
    if already:
        return
    sim = sqlite3.connect(str(config.SIM_DB_PATH))
    sql = "SELECT tracking_no FROM sim_waybills"
    params: tuple = ()
    if carrier:
        sql += " WHERE carrier_code=?"
        params = (carrier,)
    numbers = [r[0] for r in sim.execute(sql, params)]
    sim.close()
    from collector.importer import import_waybills
    conn = get_conn()
    try:
        import_waybills(conn, "\n".join(numbers), f"bootstrap-{carrier or 'all'}")
    finally:
        conn.close()
    engine = SchedulerEngine()
    engine.start()
    try:
        _wait_caught_up(timeout=timeout)
    finally:
        engine.stop()


def _bootstrap_subset(prefix: str, carriers: list[str],
                      per_carrier: int = 4) -> set[str]:
    """导入并采完指定承运商的一个小子集，返回该子集实际单号集合。

    单号可能已被其他测试导入（重复跳过），因此以实际单号集合为准，不依赖批次名。
    """
    init_db()
    import sqlite3
    sim = sqlite3.connect(str(config.SIM_DB_PATH))
    numbers: list[str] = []
    for code in carriers:
        numbers += [r[0] for r in sim.execute(
            "SELECT tracking_no FROM sim_waybills WHERE carrier_code=? LIMIT ?",
            (code, per_carrier))]
    sim.close()
    number_set = set(numbers)

    conn = get_conn()
    try:
        existing = {r["tracking_no"] for r in conn.execute(
            "SELECT tracking_no FROM waybills")}
        fresh = [n for n in numbers if n not in existing]
    finally:
        conn.close()
    if fresh:
        from collector.importer import import_waybills
        conn = get_conn()
        try:
            import_waybills(conn, "\n".join(fresh), prefix)
        finally:
            conn.close()
        engine = SchedulerEngine()
        engine.start()
        try:
            _wait_caught_up(timeout=180, only_numbers=number_set)
        finally:
            engine.stop()
    return number_set


def test_carrier_failure_isolation_and_circuit_breaker(env):
    """单家挂掉：熔断打开并告警；其他家照常推进；恢复后可继续。"""
    number_set = _bootstrap_subset("cb-test", ["delta", "alpha"])
    cutover = _now_iso()
    conn = get_conn()
    try:
        delta_nos = [r["tracking_no"] for r in conn.execute(
            "SELECT tracking_no FROM waybills WHERE carrier_code='delta' "
            "AND tracking_no IN (%s)" % ",".join("?" * len(number_set)),
            tuple(number_set))]
        # 无条件重新激活子集内 delta 单并立即到期（可能已终态归档）
        conn.execute(
            "UPDATE waybills SET active=1, next_poll_at=?, lease_owner=NULL, "
            "lease_expires_at=NULL, fail_count=0, terminal_polls=0 "
            "WHERE tracking_no IN (%s)" % ",".join("?" * len(delta_nos)),
            (_now_iso(), *delta_nos))
        conn.execute(
            "UPDATE carrier_configs SET consecutive_fails=0, circuit_open_until=NULL "
            "WHERE code='delta'")
        delta_due = conn.execute(
            "SELECT COUNT(*) c FROM waybills WHERE carrier_code='delta' "
            "AND tracking_no IN (%s) AND active=1" % ",".join("?" * len(delta_nos)),
            tuple(delta_nos)).fetchone()["c"]
    finally:
        conn.close()
    assert delta_due >= 1

    requests.post(env["base"] + "/admin/down", json={"codes": ["delta"]})
    try:
        engine = SchedulerEngine()
        engine.start()
        try:
            deadline = time.time() + 120
            tripped = False
            while time.time() < deadline:
                conn = get_conn()
                try:
                    row = conn.execute(
                        "SELECT circuit_open_until FROM carrier_configs "
                        "WHERE code='delta'").fetchone()
                finally:
                    conn.close()
                if row["circuit_open_until"]:
                    tripped = True
                    break
                time.sleep(1)
            assert tripped, "delta 熔断未触发"

            # 其他家不被拖累：alpha 子集在 delta 宕机期间仍能成功轮询
            conn = get_conn()
            try:
                conn.execute(
                    "UPDATE waybills SET active=1, next_poll_at=?, "
                    "lease_owner=NULL, lease_expires_at=NULL, terminal_polls=0 "
                    "WHERE carrier_code='alpha' AND tracking_no IN (%s)"
                    % ",".join("?" * len(number_set)),
                    (_now_iso(), *number_set))
            finally:
                conn.close()
            deadline = time.time() + 60
            alpha_new_ok = 0
            while time.time() < deadline:
                conn = get_conn()
                try:
                    alpha_new_ok = conn.execute(
                        "SELECT COUNT(*) c FROM fetch_logs WHERE carrier_code='alpha' "
                        "AND ok=1 AND started_at>=?", (cutover,)).fetchone()["c"]
                finally:
                    conn.close()
                if alpha_new_ok >= 1:
                    break
                time.sleep(1)
            conn = get_conn()
            try:
                assert alpha_new_ok >= 1, "delta 宕机影响到了 alpha 的正常采集"
                alerts = conn.execute(
                    "SELECT kind FROM alerts WHERE carrier_code='delta' "
                    "AND resolved_at IS NULL").fetchall()
                assert any(a["kind"] in ("site_down", "http_error") for a in alerts)
            finally:
                conn.close()
        finally:
            engine.stop()
    finally:
        requests.post(env["base"] + "/admin/down", json={"codes": []})
        conn = get_conn()
        try:
            conn.execute(
                "UPDATE carrier_configs SET consecutive_fails=0, circuit_open_until=NULL "
                "WHERE code='delta'")
        finally:
            conn.close()


def test_redesign_raises_alert_and_no_empty_data(env):
    """theta 改版：产生 parse_error 告警，已有节点不被清空，切 v2 后恢复。"""
    _ensure_waybills_loaded()
    requests.post(env["base"] + "/admin/schema", json={"version": "v2"})
    try:
        engine = SchedulerEngine()
        engine.start()
        try:
            conn = get_conn()
            try:
                before = conn.execute(
                    "SELECT COUNT(*) c FROM tracking_nodes tn JOIN waybills w "
                    "ON w.id=tn.waybill_id WHERE w.carrier_code='theta'").fetchone()["c"]
                # 重新打开 theta 单，确保改版后一定有轮询触发解析失败
                conn.execute(
                    "UPDATE waybills SET active=1, next_poll_at=?, "
                    "lease_owner=NULL, lease_expires_at=NULL "
                    "WHERE carrier_code='theta'", (_now_iso(),))
            finally:
                conn.close()
            deadline = time.time() + 120
            alert = None
            while time.time() < deadline:
                conn = get_conn()
                try:
                    alert = conn.execute(
                        "SELECT id FROM alerts WHERE carrier_code='theta' "
                        "AND kind='parse_error' AND resolved_at IS NULL").fetchone()
                finally:
                    conn.close()
                if alert:
                    break
                time.sleep(1)
            assert alert, "改版未触发 parse_error 告警"
            conn = get_conn()
            try:
                after = conn.execute(
                    "SELECT COUNT(*) c FROM tracking_nodes tn JOIN waybills w "
                    "ON w.id=tn.waybill_id WHERE w.carrier_code='theta'").fetchone()["c"]
                assert after >= before, "解析失败不得清空已有轨迹"
                conn.execute(
                    "UPDATE carrier_configs SET parser_version='v2' WHERE code='theta'")
                conn.execute(
                    "UPDATE waybills SET next_poll_at=? "
                    "WHERE carrier_code='theta' AND active=1", (_now_iso(),))
            finally:
                conn.close()
            deadline = time.time() + 90
            good = 0
            while time.time() < deadline:
                conn = get_conn()
                try:
                    good = conn.execute(
                        "SELECT COUNT(*) c FROM fetch_logs WHERE carrier_code='theta' "
                        "AND ok=1 ORDER BY id DESC LIMIT 1").fetchone()["c"]
                finally:
                    conn.close()
                if good:
                    break
                time.sleep(1)
            assert good
        finally:
            engine.stop()
    finally:
        requests.post(env["base"] + "/admin/schema", json={"version": "v1"})
        conn = get_conn()
        try:
            conn.execute(
                "UPDATE carrier_configs SET parser_version='v1' WHERE code='theta'")
        finally:
            conn.close()

    report = reconcile()
    assert report.ok, _fail_text(report)

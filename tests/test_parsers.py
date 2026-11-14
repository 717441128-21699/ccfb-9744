"""解析器测试：对活体仿真站逐家抓取并解析，校验结构/时区/状态映射。"""
from __future__ import annotations

import sqlite3

import pytest
import requests

from collector import canonical
from collector.fetcher import CarrierFetcher
from collector.parsers import ParseError, parse_payload
from sim import simlib


def _one_waybill(carrier: str, env) -> str:
    conn = sqlite3.connect(str(env["config"].SIM_DB_PATH))
    no = conn.execute(
        "SELECT tracking_no FROM sim_waybills WHERE carrier_code=? LIMIT 1",
        (carrier,)).fetchone()[0]
    conn.close()
    return no


@pytest.mark.parametrize("carrier", ["alpha", "beta", "gamma", "delta",
                                     "epsilon", "zeta", "eta", "theta"])
def test_fetch_and_parse_each_carrier(carrier, env):
    from collector.carriers import CARRIERS
    from collector.bootstrap import bootstrap_carriers
    from collector.db import get_conn, init_db

    init_db(env["config"].DB_PATH)
    conn = get_conn(env["config"].DB_PATH)
    bootstrap_carriers(conn, env["base"])
    cfg = dict(conn.execute(
        "SELECT * FROM carrier_configs WHERE code=?", (carrier,)).fetchone())
    conn.close()

    spec = CARRIERS[carrier]
    fetcher = CarrierFetcher(
        carrier, base_url=env["base"], mode=cfg["mode"],
        needs_login=spec.needs_login, login_user=spec.login_user,
        login_pass=spec.login_pass, rate_rps=spec.rate_rps,
        sign_secret=spec.params.get("sign_secret"))
    fetcher.start()
    try:
        no = _one_waybill(carrier, env)
        result = fetcher.fetch(no)
        assert result.ok, f"{carrier} 抓取失败: {result.error_kind} {result.error_message}"
        nodes = parse_payload(carrier, result.payload, "v1")
        if hasattr(result.payload, "close"):
            result.payload.close()
        assert len(nodes) >= 1
        for n in nodes:
            assert n.status != Status_UNKNOWN(), n
            # 时间必须是带偏移的 UTC ISO
            assert n.event_time_utc.endswith("+00:00")
            assert n.uid and n.description
    finally:
        fetcher.close()


def Status_UNKNOWN():
    return canonical.Status.UNKNOWN.value


def test_theta_redesign_breaks_v1_parser_and_v2_recovers(env):
    """改版后 v1 解析器必须抛 ParseError（告警链路），切 v2 后恢复。"""
    base = env["base"]
    requests.post(base + "/admin/schema", json={"version": "v2"})
    try:
        no = _one_waybill("theta", env)
        fetcher = CarrierFetcher("theta", base_url=base,
                                 mode="browser_page")
        fetcher.start()
        try:
            result = fetcher.fetch(no)
            assert result.ok
            with pytest.raises(ParseError):
                page = result.payload
                parse_payload("theta", page, "v1")
            page.close()
            result2 = fetcher.fetch(no)
            nodes = parse_payload("theta", result2.payload, "v2")
            assert nodes and nodes[0].status != "UNKNOWN"
            result2.payload.close()
        finally:
            fetcher.close()
    finally:
        requests.post(base + "/admin/schema", json={"version": "v1"})


def test_delta_rate_limit_returns_429_under_burst(env):
    """delta 严格限流：突发请求里应当出现 429（由 worker 退避消化）。"""
    no = _one_waybill("delta", env)
    statuses = []
    for _ in range(8):
        r = requests.get(env["base"] + "/delta/api/eventos",
                         params={"numero": no}, timeout=10)
        statuses.append(r.status_code)
    assert 429 in statuses


def test_gamma_rejects_bad_signature(env):
    no = _one_waybill("gamma", env)
    r = requests.get(env["base"] + "/gamma/api/v1/trace",
                     params={"tracking_no": no, "ts": "2026-11-14T10:00:00+00:00",
                             "nonce": "x", "sign": "deadbeef"}, timeout=10)
    assert r.status_code == 401

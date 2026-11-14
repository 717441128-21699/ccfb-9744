"""各家页面/接口的渲染器：同样的真值，八种完全不同的结构。"""
from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3
from datetime import datetime, timezone

from collector import canonical
from collector.carriers import CARRIERS
from . import simlib


# ---------------- HTML 四家 ----------------

def _page_shell(title: str, body: str) -> str:
    return f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<title>{title}</title></head><body>{body}</body></html>"""


def render_alpha(conn: sqlite3.Connection, tracking_no: str) -> str:
    rows = simlib.visible_events(conn, tracking_no)
    trs = "".join(
        f"<tr><td class='ev-time'>{simlib.render_local('alpha', r['event_time_utc'])}</td>"
        f"<td class='ev-status' data-code='{r['raw_status']}'>{r['description']}</td>"
        f"<td class='ev-loc'>{r['location']}</td></tr>" for r in rows)
    body = f"""
    <div id="alpha-app">
      <h1>Alpha 速运</h1>
      <p>运单号：<span id="track-no">{tracking_no}</span></p>
      <table class="track-table"><thead><tr><th>时间</th><th>状态</th><th>地点</th></tr></thead>
      <tbody>{trs}</tbody></table>
    </div>"""
    return _page_shell("Alpha Tracking", body)


def render_beta(conn: sqlite3.Connection, tracking_no: str) -> str:
    rows = simlib.visible_events(conn, tracking_no)
    items = "".join(
        f"""<li class="step">
          <span class="step-time">{simlib.render_local('beta', r['event_time_utc'])}</span>
          <span class="step-state">{r['raw_status']}</span>
          <span class="step-desc">{r['description']}</span>
          <span class="step-place">{r['location']}</span>
        </li>""" for r in rows)
    body = f"""
    <section data-provider="beta">
      <h2>Beta Parcels Tracking</h2>
      <div class="consignment">{tracking_no}</div>
      <ul class="steps">{items}</ul>
    </section>"""
    return _page_shell("Beta Tracking", body)


def render_epsilon(conn: sqlite3.Connection, tracking_no: str) -> str:
    rows = simlib.visible_events(conn, tracking_no)
    items = "".join(
        f"""<div class="ryaku-card">
          <div class="ryaku-date">{simlib.render_local('epsilon', r['event_time_utc'])}</div>
          <div class="ryaku-joutai">{r['raw_status']}</div>
          <div class="ryaku-basho">{r['location']}</div>
          <div class="ryaku-memo">{r['description']}</div>
        </div>""" for r in rows)
    body = f"<h1>Epsilon 運輸 追跡</h1><p>問合せ番号：{tracking_no}</p>{items}"
    return _page_shell("Epsilon 追跡", body)


def render_theta(conn: sqlite3.Connection, tracking_no: str, version: str) -> str:
    """theta 会改版：v1 是 table 结构，v2 改成 div+data 属性的全新结构。"""
    rows = simlib.visible_events(conn, tracking_no)
    if version == "v1":
        blocks = "".join(
            f"<tr class='old-row'><td>{simlib.render_local('theta', r['event_time_utc'])}</td>"
            f"<td>{r['raw_status']}</td><td>{r['location']}</td>"
            f"<td>{r['description']}</td></tr>" for r in rows)
        body = f"<table id='legacy-trace'><tbody>{blocks}</tbody></table>"
        return _page_shell("Theta legacy", body)
    # v2：完全不同的 DOM，状态还套了一层前缀
    blocks = "".join(
        f"""<div class="trace-item" data-seq="{r['seq']}">
          <time data-utc="{r['event_time_utc']}">{simlib.render_local('theta', r['event_time_utc'])}</time>
          <span class="badge badge-{r['seq']}">TH::{r['raw_status']}</span>
          <div class="where">{r['location']}</div>
          <p class="what">{r['description']}</p>
        </div>""" for r in rows)
    body = f"""<main id="theta-next">
      <header><b>Theta Mail</b><i>{tracking_no}</i></header>
      <div class="timeline">{blocks}</div></main>"""
    return _page_shell("Theta Next", body)


# ---------------- JSON 四家 ----------------

def build_gamma(conn: sqlite3.Connection, tracking_no: str) -> dict:
    rows = simlib.visible_events(conn, tracking_no)
    return {
        "waybill": tracking_no,
        "tracePoints": [
            {
                "pointId": r["uid"],
                "orderNo": r["seq"],
                "code": r["raw_status"],                 # 数字状态码
                "happenedAt": simlib.render_local("gamma", r["event_time_utc"]),
                "city": r["location"],
                "memo": r["description"],
            } for r in rows
        ],
    }


def build_delta(conn: sqlite3.Connection, tracking_no: str) -> dict:
    rows = simlib.visible_events(conn, tracking_no)
    return {
        "numero": tracking_no,
        "eventos": [
            {
                "id": r["uid"],
                "ordem": r["seq"],
                "situacao": r["raw_status"],
                "dataHora": simlib.render_local("delta", r["event_time_utc"]),
                "local": r["location"],
                "detalhe": r["description"],
            } for r in rows
        ],
    }


def build_zeta(conn: sqlite3.Connection, tracking_no: str) -> dict:
    rows = simlib.visible_events(conn, tracking_no)
    return {
        "shipment": tracking_no,
        "nodes": [
            {
                "nodeUid": r["uid"],
                "position": r["seq"],
                "state": r["raw_status"],
                "timestamp": simlib.render_local("zeta", r["event_time_utc"]),
                "hub": r["location"],
                "info": r["description"],
            } for r in rows
        ],
    }


def build_eta(conn: sqlite3.Connection, tracking_no: str) -> dict:
    # eta 是整单快照接口：直接给当前完整快照（撤回的节点天然消失）
    rows = simlib.visible_events(conn, tracking_no)
    return {
        "consignment": tracking_no,
        "snapshotGeneratedAt": canonical.to_utc_iso(canonical.now_utc()),
        "items": [
            {
                "itemUid": r["uid"],
                "idx": r["seq"],
                "phase": r["raw_status"],
                "atLocal": simlib.render_local("eta", r["event_time_utc"]),
                "station": r["location"],
                "note": r["description"],
            } for r in rows
        ],
    }


# ---------------- 签名 ----------------

def gamma_sign(tracking_no: str, ts: str, nonce: str) -> str:
    secret = CARRIERS["gamma"].params["sign_secret"]
    msg = f"{tracking_no}|{ts}|{nonce}".encode()
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()


def verify_gamma(tracking_no: str, ts: str, nonce: str, sign: str) -> bool:
    # 时间戳允许 5 分钟偏差
    try:
        ts_dt = datetime.fromisoformat(ts)
        if ts_dt.tzinfo is None:
            return False
        if abs((canonical.now_utc() - ts_dt.astimezone(timezone.utc)).total_seconds()) > 300:
            return False
    except ValueError:
        return False
    expect = gamma_sign(tracking_no, ts, nonce, )
    return hmac.compare_digest(expect, sign or "")


def zeta_page_token(tracking_no: str) -> str:
    """zeta 页面里埋的一次性 token，调 JSON 接口必须带上。"""
    return hashlib.sha1(f"zeta-{tracking_no}".encode()).hexdigest()[:16]

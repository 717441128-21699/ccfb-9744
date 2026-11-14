"""各承运商页面/JSON 解析器。

契约：
- HTML 解析器接收 Playwright Page（worker 持有浏览器，避免每单重启 Chromium）；
- JSON 解析器接收 dict；
- 输出 list[CanonicalNode]，时间统一为 UTC ISO，状态统一为规范码；
- 页面结构与预期不符（改版/被反爬页替换/空壳页）抛 ParseError；
- 原始状态无法映射不抛错，但节点状态为 UNKNOWN，由上层告警。
"""
from __future__ import annotations

from playwright.sync_api import Page

from . import canonical
from .canonical import CanonicalNode
from sim.simlib import TIME_FORMATS


class ParseError(Exception):
    """结构解析失败：调用方必须告警且不得写入空轨迹。"""


def _make_node(carrier: str, uid: str, seq: int, local_time: str,
               raw_status: str, description: str, location: str) -> CanonicalNode:
    dt = canonical.parse_local(carrier, local_time, TIME_FORMATS[carrier])
    status = canonical.map_status(carrier, raw_status)
    return CanonicalNode(
        uid=uid, seq=seq,
        event_time_utc=canonical.to_utc_iso(dt),
        status=status.value,
        description=description or raw_status,
        location=location or "",
    )


def _dom_nodes(page: Page, carrier: str, locator_str: str, field_selectors: list[tuple[str, str]],
               uid_prefix: str, *, seq_attr: str | None = None) -> list[CanonicalNode]:
    """通用 DOM 抽取：field_selectors 为 (key, css) 列表，须包含 time/raw/desc/loc。"""
    elements = page.locator(locator_str).all()
    if not elements:
        raise ParseError(f"{carrier}: 选择器 {locator_str} 未命中任何节点")
    nodes = []
    for index, el in enumerate(elements, start=1):
        values: dict[str, str] = {}
        for key, sel in field_selectors:
            if "@" in sel:
                css, attr = sel.split("@", 1)
                target = el if css == "" else el.locator(css).first
                values[key] = target.get_attribute(attr) or ""
            else:
                values[key] = el.locator(sel).first.text_content() or ""
        if not values.get("time") or not values.get("raw"):
            raise ParseError(f"{carrier}: 第 {index} 个节点字段缺失")
        seq = int(el.get_attribute(seq_attr)) if seq_attr else index
        nodes.append(_make_node(
            carrier, f"{uid_prefix}-{seq:02d}", seq,
            values["time"].strip(), values["raw"].strip(),
            values.get("desc", "").strip(), values.get("loc", "").strip()))
    return nodes


# ---------------- HTML 解析 ----------------

def parse_alpha(page: Page) -> list[CanonicalNode]:
    page.wait_for_selector("table.track-table tbody tr", timeout=8000)
    return _dom_nodes(
        page, "alpha", "table.track-table tbody tr",
        [("time", "td.ev-time"), ("raw", "td.ev-status@data-code"),
         ("desc", "td.ev-status"), ("loc", "td.ev-loc")],
        "alpha")


def parse_beta(page: Page) -> list[CanonicalNode]:
    page.wait_for_selector("ul.steps li.step", timeout=8000)
    return _dom_nodes(
        page, "beta", "ul.steps li.step",
        [("time", ".step-time"), ("raw", ".step-state"),
         ("desc", ".step-desc"), ("loc", ".step-place")],
        "beta")


def parse_epsilon(page: Page) -> list[CanonicalNode]:
    page.wait_for_selector("div.ryaku-card", timeout=8000)
    return _dom_nodes(
        page, "epsilon", "div.ryaku-card",
        [("time", ".ryaku-date"), ("raw", ".ryaku-joutai"),
         ("loc", ".ryaku-basho"), ("desc", ".ryaku-memo")],
        "epsilon")


def parse_theta(page: Page, version: str) -> list[CanonicalNode]:
    if version == "v1":
        # 改版后旧选择器会消失：只等很短时间，快速抛 ParseError 走告警链路
        try:
            page.wait_for_selector("table#legacy-trace tr.old-row", timeout=3000)
        except Exception as e:
            raise ParseError("theta(v1): legacy-trace 行缺失，疑似改版") from e
        rows = page.locator("table#legacy-trace tr.old-row").all()
        nodes = []
        for seq, row in enumerate(rows, start=1):
            tds = row.locator("td").all()
            if len(tds) < 4:
                raise ParseError("theta(v1): 列数不足，疑似改版")
            t, raw, loc, desc = ((td.text_content() or "").strip() for td in tds[:4])
            if not t or not raw:
                raise ParseError("theta(v1): 字段缺失，疑似改版")
            nodes.append(_make_node("theta", f"theta-{seq:02d}", seq,
                                    t, raw, desc, loc))
        return nodes
    # v2：改版后的全新结构，状态多了 TH:: 前缀
    page.wait_for_selector("#theta-next div.trace-item", timeout=8000)
    items = page.locator("#theta-next div.trace-item").all()
    nodes = []
    for item in items:
        seq = int(item.get_attribute("data-seq"))
        t = (item.locator("time").text_content() or "").strip()
        badge = (item.locator("span.badge").text_content() or "").strip()
        if not badge.startswith("TH::"):
            raise ParseError("theta(v2): badge 前缀不符")
        raw = badge.removeprefix("TH::").strip()
        loc = (item.locator(".where").text_content() or "").strip()
        desc = (item.locator(".what").text_content() or "").strip()
        nodes.append(_make_node("theta", f"theta-{seq:02d}", seq, t, raw, desc, loc))
    nodes.sort(key=lambda n: n.seq)
    return nodes


# ---------------- JSON 解析 ----------------

def _json_nodes(payload: dict, carrier: str, list_key: str, fields: dict[str, str],
                seq_key: str, uid_key: str) -> list[CanonicalNode]:
    rows = payload.get(list_key)
    if not isinstance(rows, list) or not rows:
        raise ParseError(f"{carrier}: {list_key} 缺失或为空")
    nodes = []
    for row in rows:
        try:
            seq = int(row[seq_key])
            nodes.append(_make_node(
                carrier, str(row[uid_key]), seq,
                str(row[fields["time"]]), str(row[fields["raw"]]),
                str(row.get(fields["desc"], "")), str(row.get(fields["loc"], ""))))
        except (KeyError, TypeError, ValueError) as e:
            raise ParseError(f"{carrier}: 节点字段异常 {e}") from e
    return nodes


def parse_gamma(payload: dict) -> list[CanonicalNode]:
    return _json_nodes(payload, "gamma", "tracePoints",
                       {"time": "happenedAt", "raw": "code",
                        "desc": "memo", "loc": "city"},
                       "orderNo", "pointId")


def parse_delta(payload: dict) -> list[CanonicalNode]:
    return _json_nodes(payload, "delta", "eventos",
                       {"time": "dataHora", "raw": "situacao",
                        "desc": "detalhe", "loc": "local"},
                       "ordem", "id")


def parse_zeta(payload: dict) -> list[CanonicalNode]:
    return _json_nodes(payload, "zeta", "nodes",
                       {"time": "timestamp", "raw": "state",
                        "desc": "info", "loc": "hub"},
                       "position", "nodeUid")


def parse_eta(payload: dict) -> list[CanonicalNode]:
    return _json_nodes(payload, "eta", "items",
                       {"time": "atLocal", "raw": "phase",
                        "desc": "note", "loc": "station"},
                       "idx", "itemUid")


def parse_payload(carrier: str, payload, parser_version: str = "v1") -> list[CanonicalNode]:
    if carrier == "alpha":
        return parse_alpha(payload)
    if carrier == "beta":
        return parse_beta(payload)
    if carrier == "gamma":
        return parse_gamma(payload)
    if carrier == "delta":
        return parse_delta(payload)
    if carrier == "epsilon":
        return parse_epsilon(payload)
    if carrier == "zeta":
        return parse_zeta(payload)
    if carrier == "eta":
        return parse_eta(payload)
    if carrier == "theta":
        return parse_theta(payload, parser_version)
    raise ParseError(f"unknown carrier {carrier}")

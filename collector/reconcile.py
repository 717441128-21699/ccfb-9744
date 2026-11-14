"""逐单对账：采集库 vs 仿真站真值库。

比对维度：
1. 运单归属承运商、当前规范状态；
2. 每个有效节点的 uid / seq / UTC 事件时间 / 规范状态；
3. 撤回节点：真值撤回的，采集库必须 retracted=1（eta 场景）；
4. 采集库不得有真值不存在的节点；
5. 终态运单必须已归档（active=0），在途运单必须仍 active。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from . import canonical, config
from .canonical import map_status
from .db import get_conn


@dataclass
class ReconcileReport:
    waybills_checked: int = 0
    nodes_checked: int = 0
    mismatches: list[dict] = field(default_factory=list)
    missing_waybills: list[str] = field(default_factory=list)
    extra_waybills: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.mismatches or self.missing_waybills or self.extra_waybills)

    def add(self, tracking_no: str, problem: str, **detail) -> None:
        self.mismatches.append({"tracking_no": tracking_no,
                                "problem": problem, **detail})


def _norm_iso(s: str) -> str:
    """双方都归一到 UTC ISO（秒级），消除不同时区表达带来的假差异。"""
    from datetime import datetime
    return canonical.to_utc_iso(datetime.fromisoformat(s))


def reconcile(sim_db_path=None, collector_db_path=None) -> ReconcileReport:
    report = ReconcileReport()
    sim = sqlite3.connect(str(sim_db_path or config.SIM_DB_PATH))
    sim.row_factory = sqlite3.Row
    col = get_conn(collector_db_path)
    try:
        sim_rows = sim.execute(
            "SELECT tracking_no, carrier_code, final_status FROM sim_waybills ORDER BY tracking_no"
        ).fetchall()
        sim_nos = {r["tracking_no"] for r in sim_rows}

        col_rows = col.execute(
            "SELECT id, tracking_no, carrier_code, current_status, active, "
            "terminal_polls FROM waybills"
        ).fetchall()
        col_map = {r["tracking_no"]: r for r in col_rows}

        report.extra_waybills = sorted(set(col_map) - sim_nos)
        report.missing_waybills = sorted(sim_nos - set(col_map))

        for sr in sim_rows:
            no = sr["tracking_no"]
            cr = col_map.get(no)
            if cr is None:
                continue
            report.waybills_checked += 1
            if cr["carrier_code"] != sr["carrier_code"]:
                report.add(no, "carrier_mismatch",
                           truth=sr["carrier_code"], got=cr["carrier_code"])
            if cr["current_status"] != sr["final_status"]:
                report.add(no, "status_mismatch",
                           truth=sr["final_status"], got=cr["current_status"])
            expect_terminal = sr["final_status"] in canonical.TERMINAL_STATUSES
            # 终态单 active=1 表示仍处于宽限复核窗口（复核用尽后调度器会置 active=0），
            # 这是合法过渡态，不计差异。
            in_grace = expect_terminal and bool(cr["active"])
            if not in_grace and bool(cr["active"]) == expect_terminal:
                report.add(no, "active_flag_wrong",
                           terminal=expect_terminal, active=cr["active"],
                           terminal_polls=cr["terminal_polls"])

            # 节点级比对
            truth_events = sim.execute(
                "SELECT * FROM sim_events WHERE tracking_no=? ORDER BY seq", (no,)
            ).fetchall()
            got_nodes = col.execute(
                "SELECT * FROM tracking_nodes WHERE waybill_id=? ORDER BY seq",
                (cr["id"],)
            ).fetchall()
            got_by_uid = {n["uid"]: n for n in got_nodes}

            for ev in truth_events:
                report.nodes_checked += 1
                gn = got_by_uid.get(ev["uid"])
                if gn is None:
                    # 还没到可见时间的节点，采集端本来就看不到，不算差异
                    if ev["visible_at"] <= canonical.to_utc_iso(canonical.now_utc()) \
                            and not ev["retracted"]:
                        report.add(no, "missing_visible_node", uid=ev["uid"])
                    continue
                expect_status = map_status(sr["carrier_code"], ev["raw_status"]).value
                if gn["status"] != expect_status:
                    report.add(no, "node_status_mismatch", uid=ev["uid"],
                               truth=expect_status, got=gn["status"])
                if _norm_iso(gn["event_time_utc"]) != _norm_iso(ev["event_time_utc"]):
                    report.add(no, "node_time_mismatch", uid=ev["uid"],
                               truth=ev["event_time_utc"], got=gn["event_time_utc"])
                if gn["seq"] != ev["seq"]:
                    report.add(no, "node_seq_mismatch", uid=ev["uid"],
                               truth=ev["seq"], got=gn["seq"])
                if bool(ev["retracted"]) != bool(gn["retracted"]):
                    report.add(no, "retract_flag_mismatch", uid=ev["uid"],
                               truth=bool(ev["retracted"]), got=bool(gn["retracted"]))

            truth_uids = {ev["uid"] for ev in truth_events}
            for uid, gn in got_by_uid.items():
                if uid not in truth_uids:
                    report.add(no, "extra_node", uid=uid)
        return report
    finally:
        sim.close()
        col.close()


def format_report(report: ReconcileReport) -> str:
    lines = [
        "==== 对账报告 ====",
        f"运单数: {report.waybills_checked}，节点数: {report.nodes_checked}",
        f"漏采运单: {len(report.missing_waybills)}，多采运单: {len(report.extra_waybills)}",
        f"差异条目: {len(report.mismatches)}",
    ]
    for no in report.missing_waybills[:20]:
        lines.append(f"  [漏采] {no}")
    for no in report.extra_waybills[:20]:
        lines.append(f"  [多采] {no}")
    for m in report.mismatches[:50]:
        lines.append(f"  [差异] {m}")
    lines.append("结论: " + ("✅ 差异为 0" if report.ok else "❌ 存在差异"))
    return "\n".join(lines)

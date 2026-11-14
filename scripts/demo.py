r"""端到端演示：生成仿真数据 -> 起仿真站 -> 导入(含坏单号) -> 采集 -> 对账。

独立于 pytest，使用隔离的数据目录和加速时间尺度。
用法: py -3 scripts\demo.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 必须在 import collector 之前配置环境
DEMO_DIR = Path(tempfile.mkdtemp(prefix="collector-demo-"))
os.environ["COLLECTOR_DATA_DIR"] = str(DEMO_DIR)
os.environ["COLLECTOR_TIME_SCALE"] = "0.05"
os.environ["SIM_PORT"] = "8770"

from collector import canonical, config  # noqa: E402

# 演示加速：缩短在途轮询与终态宽限，保证能追上分批开放的节点
config.FAST_POLL_SECONDS = 2
config.MAX_REMINDER_BACKOFF = 8
config.LEASE_SECONDS = 5
config.TERMINAL_GRACE_POLLS = 2
config.TERMINAL_GRACE_INTERVAL = 3
for _k, _v in {"PENDING": 4, "PICKED_UP": 3, "IN_TRANSIT": 3, "CUSTOMS": 6,
               "OUT_FOR_DELIVERY": 3, "EXCEPTION": 4, "UNKNOWN": 6}.items():
    config.POLL_INTERVALS_SECONDS[_k] = _v

from collector.bootstrap import bootstrap_carriers  # noqa: E402
from collector.db import get_conn, init_db  # noqa: E402
from collector.importer import import_waybills  # noqa: E402
from collector.reconcile import format_report, reconcile  # noqa: E402
from collector.scheduler import SchedulerEngine  # noqa: E402
from sim.app import app as sim_app  # noqa: E402
from sim.generate import generate  # noqa: E402
from werkzeug.serving import make_server  # noqa: E402


def main() -> int:
    print(f"演示数据目录: {DEMO_DIR}")
    stats = generate(per_carrier=10)
    print("仿真真值生成:", stats)

    init_db()
    conn = get_conn()
    bootstrap_carriers(conn, config.SIM_BASE)
    conn.close()

    server = make_server(config.SIM_HOST, config.SIM_PORT, sim_app, threaded=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.5)

    # 从真值库取全部单号 + 3 个识别不了的坏单号
    import sqlite3
    sim = sqlite3.connect(str(config.SIM_DB_PATH))
    numbers = [r[0] for r in sim.execute(
        "SELECT tracking_no FROM sim_waybills ORDER BY tracking_no")]
    sim.close()
    numbers += ["BAD-0001", "WRONG99999X", "YP_SHORT"]

    conn = get_conn()
    info = import_waybills(conn, "\n".join(numbers), "demo")
    conn.close()
    print("导入结果:", info)

    engine = SchedulerEngine()
    engine.start()
    try:
        # 以仿真真值为收敛标准：有效节点数 + 撤回标记都对齐才算采完
        import sqlite3 as _sq
        deadline = time.time() + 300
        while time.time() < deadline:
            sdb = _sq.connect(str(config.SIM_DB_PATH))
            sdb.row_factory = _sq.Row
            truth = sdb.execute(
                "SELECT tracking_no, "
                "SUM(CASE WHEN retracted=0 AND visible_at<=? THEN 1 ELSE 0 END) an, "
                "SUM(CASE WHEN retracted=1 THEN 1 ELSE 0 END) rn "
                "FROM sim_events GROUP BY tracking_no",
                (canonical.to_utc_iso(canonical.now_utc()),)).fetchall()
            sdb.close()
            conn = get_conn()
            done = True
            for tr in truth:
                got = conn.execute(
                    "SELECT SUM(CASE WHEN n.retracted=0 THEN 1 ELSE 0 END) an, "
                    "SUM(CASE WHEN n.retracted=1 THEN 1 ELSE 0 END) rn "
                    "FROM tracking_nodes n JOIN waybills w ON w.id=n.waybill_id "
                    "WHERE w.tracking_no=?", (tr["tracking_no"],)).fetchone()
                if (got["an"] or 0) != (tr["an"] or 0) or \
                   (got["rn"] or 0) != (tr["rn"] or 0):
                    done = False
                    break
            conn.close()
            if done:
                break
            time.sleep(1)
    finally:
        engine.stop()
    server.shutdown()

    report = reconcile()
    print(format_report(report))

    conn = get_conn()
    dup = conn.execute(
        "SELECT COUNT(*) c FROM (SELECT waybill_id, uid FROM tracking_nodes "
        "GROUP BY waybill_id, uid HAVING COUNT(*)>1)").fetchone()["c"]
    unrec = conn.execute("SELECT COUNT(*) c FROM unrecognized_waybills").fetchone()["c"]
    alerts = conn.execute(
        "SELECT COUNT(*) c FROM alerts WHERE resolved_at IS NULL").fetchone()["c"]
    print(f"重复节点: {dup}，无法识别单号: {unrec}，未解决告警: {alerts}")
    conn.close()
    return 0 if report.ok and dup == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

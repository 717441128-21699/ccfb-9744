"""命令行工具：导入 / 推进仿真轨迹 / 对账 / 重置。

用法：
  py -3 cli.py import 文件或-
  py -3 cli.py import-sample          # 从仿真真值库导入全部单号 + 3 个坏单号
  py -3 cli.py advance [N]            # 让仿真站轨迹追加新节点
  py -3 cli.py reconcile              # 逐单对账
  py -3 cli.py reset                  # 清空采集库（仿真真值保留）
  py -3 cli.py gen-data [每单件数]    # 重建仿真真值
"""
from __future__ import annotations

import sqlite3
import sys

from collector import config
from collector.bootstrap import bootstrap_carriers
from collector.db import get_conn, init_db
from collector.importer import import_waybills
from collector.reconcile import format_report, reconcile
from sim.generate import generate, advance as _advance


def cmd_import(path: str) -> None:
    init_db()
    if path == "-":
        text = sys.stdin.read()
    else:
        with open(path, encoding="utf-8-sig") as f:
            text = f.read()
    conn = get_conn()
    try:
        print(import_waybills(conn, text, path))
    finally:
        conn.close()


def cmd_import_sample() -> None:
    """从仿真真值库取全部单号，再混入 3 个识别不了的坏单号。"""
    init_db()
    sim = sqlite3.connect(str(config.SIM_DB_PATH))
    numbers = [r[0] for r in sim.execute(
        "SELECT tracking_no FROM sim_waybills ORDER BY tracking_no")]
    sim.close()
    numbers += ["BAD-0001", "WRONG99999X", "YP_SHORT"]
    conn = get_conn()
    try:
        bootstrap_carriers(conn, config.SIM_BASE)
        print(import_waybills(conn, "\n".join(numbers), "sim-sample"))
    finally:
        conn.close()


def cmd_advance(n: int = 40) -> None:
    print({"added": _advance(max_new=n)})


def cmd_reconcile() -> int:
    report = reconcile()
    print(format_report(report))
    return 0 if report.ok else 1


def cmd_reset() -> None:
    for suffix in ("", "-wal", "-shm"):
        p = config.DB_PATH.with_name(config.DB_PATH.name + suffix)
        p.unlink(missing_ok=True)
    init_db()
    print("采集库已重置:", config.DB_PATH)


def cmd_gen_data(per_carrier: int = 40) -> None:
    print(generate(per_carrier=per_carrier))


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return 1
    cmd = args[0]
    if cmd == "import":
        cmd_import(args[1] if len(args) > 1 else "-")
    elif cmd == "import-sample":
        cmd_import_sample()
    elif cmd == "advance":
        cmd_advance(int(args[1]) if len(args) > 1 else 40)
    elif cmd == "reconcile":
        return cmd_reconcile()
    elif cmd == "reset":
        cmd_reset()
    elif cmd == "gen-data":
        cmd_gen_data(int(args[1]) if len(args) > 1 else 40)
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

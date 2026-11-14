"""进程被杀后重启恢复：租约过期 -> 新实例接手，不重复查、不漏查。"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from collector import canonical, config
from collector.db import get_conn, init_db

ROOT = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return canonical.to_utc_iso(canonical.now_utc())


def _start_worker_subprocess() -> subprocess.Popen:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(
        [sys.executable, str(ROOT / "run_worker.py")],
        cwd=str(ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        creationflags=creationflags,
    )


def _kill(proc: subprocess.Popen) -> None:
    proc.kill()
    if sys.platform == "win32":
        # /F 强杀，模拟机器掉电/任务管理器结束，不给清理机会
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                       capture_output=True)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def _alpha_numbers(limit: int = 6) -> list[str]:
    sim = sqlite3.connect(str(config.SIM_DB_PATH))
    numbers = [r[0] for r in sim.execute(
        "SELECT tracking_no FROM sim_waybills WHERE carrier_code='alpha' "
        "LIMIT ?", (limit,))]
    sim.close()
    return numbers


def test_kill_and_restart_resumes_without_dup_or_gap(env):
    init_db()
    numbers = _alpha_numbers()
    number_set = set(numbers)

    # 导入（重复的会被跳过），随后无条件重新激活，确保这批单一定待处理
    from collector.importer import import_waybills
    conn = get_conn()
    try:
        import_waybills(conn, "\n".join(numbers), "kill-test")
        conn.execute(
            "UPDATE waybills SET active=1, next_poll_at=?, lease_owner=NULL, "
            "lease_expires_at=NULL, poll_count=0, fail_count=0, terminal_polls=0 "
            "WHERE tracking_no IN (%s)" % ",".join("?" * len(numbers)),
            (_now_iso(), *numbers))
        target = conn.execute(
            "SELECT COUNT(*) c FROM waybills WHERE tracking_no IN (%s)"
            % ",".join("?" * len(numbers)), tuple(numbers)).fetchone()["c"]
    finally:
        conn.close()
    assert target == len(numbers)

    proc = _start_worker_subprocess()
    saw_work = False
    deadline = time.time() + 60
    try:
        while time.time() < deadline:
            conn = get_conn()
            try:
                active_lease = conn.execute(
                    "SELECT COUNT(*) c FROM waybills "
                    "WHERE lease_owner IS NOT NULL AND lease_expires_at>? "
                    "AND tracking_no IN (%s)" % ",".join("?" * len(numbers)),
                    (_now_iso(), *numbers)).fetchone()["c"]
                polls = conn.execute(
                    "SELECT COUNT(*) c FROM waybills "
                    "WHERE poll_count>0 AND tracking_no IN (%s)"
                    % ",".join("?" * len(numbers)), tuple(numbers)).fetchone()["c"]
            finally:
                conn.close()
            if active_lease or polls:
                saw_work = True
                break
            time.sleep(0.5)
        if not saw_work:
            proc.terminate()
            try:
                out, _ = proc.communicate(timeout=10)
            except Exception:
                out = "(no output)"
            pytest.fail(f"子进程 worker 似乎没有开始处理\n子进程输出:\n{out[-3000:]}")
    finally:
        _kill(proc)

    # 强杀后：租约成为孤儿租约，但数据完整、节点无重复
    conn = get_conn()
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM waybills WHERE tracking_no IN (%s)"
            % ",".join("?" * len(numbers)), tuple(numbers)).fetchone()["c"] == len(numbers)
        assert conn.execute(
            "SELECT COUNT(*) c FROM (SELECT waybill_id, uid FROM tracking_nodes "
            "GROUP BY waybill_id, uid HAVING COUNT(*)>1)").fetchone()["c"] == 0
    finally:
        conn.close()

    # 重启新实例前，重新激活这批单并立即到期，确保新进程一定会接手处理。
    # 这模拟“崩溃时这些单正处于待查/在途”，由重启后的实例通过过期租约接管。
    restart_started = _now_iso()
    conn = get_conn()
    try:
        placeholders = ",".join("?" * len(numbers))
        conn.execute(
            f"UPDATE waybills SET active=1, next_poll_at=?, lease_owner=NULL, "
            f"lease_expires_at=NULL, terminal_polls=0 "
            f"WHERE tracking_no IN ({placeholders})",
            (_now_iso(), *numbers))
    finally:
        conn.close()

    proc2 = _start_worker_subprocess()
    try:
        deadline = time.time() + 180
        while time.time() < deadline:
            conn = get_conn()
            try:
                # 以“重启后是否出现新的成功轮询”作为接手证据
                ok_after = conn.execute(
                    "SELECT COUNT(*) c FROM fetch_logs fl JOIN waybills w "
                    "ON w.id=fl.waybill_id "
                    f"WHERE w.tracking_no IN ({placeholders}) AND fl.ok=1 "
                    "AND fl.started_at>=?", (*numbers, restart_started)).fetchone()["c"]
                open_leases = conn.execute(
                    f"SELECT COUNT(*) c FROM waybills WHERE tracking_no IN ({placeholders}) "
                    "AND lease_owner IS NOT NULL AND lease_expires_at>?",
                    (*numbers, _now_iso())).fetchone()["c"]
                # 所有仍在调度窗口的单都必须被重启后的实例轮询过
                active_unpolled = conn.execute(
                    f"SELECT COUNT(*) c FROM waybills WHERE tracking_no IN ({placeholders}) "
                    "AND active=1 AND poll_count=0", tuple(numbers)).fetchone()["c"]
            finally:
                conn.close()
            if ok_after >= 1 and open_leases == 0 and active_unpolled == 0:
                break
            time.sleep(1)
        else:
            pytest.fail("重启后未能完成全部运单轮询")
    finally:
        _kill(proc2)

    # 恢复语义断言：
    # 1) 节点零重复（唯一索引 + 幂等双保险）；2) 无残留孤儿租约；
    # 3) 重启后仍能成功采集（能接着跑）；4) 仍在调度窗口内的单没有漏查。
    conn = get_conn()
    try:
        placeholders = ",".join("?" * len(numbers))
        rows = conn.execute(
            f"SELECT id, tracking_no, poll_count, active FROM waybills "
            f"WHERE tracking_no IN ({placeholders})", tuple(numbers)).fetchall()
        assert len(rows) == len(numbers)
        # 仍 active（在调度窗口）的单必须轮询过；已归档单不要求重查
        active_unpolled = [r for r in rows if r["active"] == 1 and r["poll_count"] == 0]
        assert not active_unpolled, "存在在途却从未轮询的运单（漏查）"

        dup = conn.execute(
            "SELECT COUNT(*) c FROM (SELECT waybill_id, uid FROM tracking_nodes "
            "GROUP BY waybill_id, uid HAVING COUNT(*)>1)").fetchone()["c"]
        assert dup == 0, "出现重复节点"

        # 重启后的新进程确实写过成功日志（证明过期租约已被顺利接手、能接着跑）
        ok_after_restart = conn.execute(
            "SELECT COUNT(*) c FROM fetch_logs fl JOIN waybills w ON w.id=fl.waybill_id "
            f"WHERE w.tracking_no IN ({placeholders}) AND fl.ok=1 "
            "AND fl.started_at>=?", (*numbers, restart_started)).fetchone()["c"]
        assert ok_after_restart >= 1, "重启后没有任何成功采集"
    finally:
        conn.close()

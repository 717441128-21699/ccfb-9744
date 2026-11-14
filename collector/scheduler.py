"""调度引擎。

架构：一个 SchedulerEngine，每个启用的承运商一条或多条 worker 线程（分片），
线程内独占一个 CarrierFetcher（含一个 Chromium 实例）。线程之间通过 SQLite
租约协作：

- claim：仅领取 next_poll_at 已到期、active=1、且租约为空/过期的运单，
  原子写入 owner+lease_expires_at；多分片用 waybill.id 取模分工；
- 进程被杀 → 租约无人续约，过期后其他/重启后的 worker 自动接手，不漏单；
- 每条运单同一时刻只有一个 worker 持有租约，不会重复查询；
- 承运商级别熔断：连续失败到阈值后 OPEN 一段时间，单家挂掉不拖累其他家。
"""
from __future__ import annotations

import json
import logging
import threading
import uuid

from . import alerts, config
from .alerts import raise_alert, resolve_alert
from .canonical import now_utc, to_utc_iso
from .datetimeutil import parse_iso, utc_plus
from .db import execute_with_retry, get_conn, transaction
from .fetcher import CarrierFetcher
from .parsers import ParseError, parse_payload
from .repository import compute_next_poll, merge_nodes

log = logging.getLogger("collector.scheduler")

# 单次领取量；多分片时小批量能让运单更均匀地分散到各 worker
CLAIM_BATCH = 3


def _empty_stat() -> dict:
    return {"polls": 0, "ok": 0, "failed": 0, "added": 0, "last_error": ""}


class SchedulerEngine:
    def __init__(self, instance_id: str | None = None) -> None:
        self.instance_id = instance_id or f"w-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._threads: dict[str, threading.Thread] = {}
        self._stats_lock = threading.Lock()
        self.stats: dict[str, dict] = {}

    # ---------- 生命周期 ----------

    def start(self) -> None:
        conn = get_conn()
        try:
            codes = [r["code"] for r in conn.execute(
                "SELECT code FROM carrier_configs WHERE enabled=1 ORDER BY code")]
        finally:
            conn.close()
        for code in codes:
            shards = config.CARRIER_WORKERS.get(code, 1)
            for shard in range(shards):
                name = f"worker-{code}" if shards == 1 else f"worker-{code}#{shard}"
                t = threading.Thread(
                    target=self._worker_loop, args=(code, shard, shards),
                    name=name, daemon=True)
                self._threads[name] = t
                with self._stats_lock:
                    self.stats[name] = _empty_stat()
                t.start()
        log.info("scheduler started: %s, carriers=%s, threads=%d",
                 self.instance_id, codes, len(self._threads))

    def stop(self, timeout: float = 10) -> None:
        self._stop.set()
        for t in self._threads.values():
            t.join(timeout=timeout)
        log.info("scheduler stopped")

    def wait_idle(self, min_idle_rounds: int = 2, poll: float = 1.0) -> None:
        """测试辅助：等到所有在途运单都没有到期任务（连续 N 轮空闲）。"""
        idle_rounds = 0
        while not self._stop.is_set():
            conn = get_conn()
            try:
                due = conn.execute(
                    "SELECT COUNT(*) c FROM waybills WHERE active=1 AND next_poll_at<=?",
                    (to_utc_iso(now_utc()),)).fetchone()["c"]
            finally:
                conn.close()
            if due == 0:
                idle_rounds += 1
                if idle_rounds >= min_idle_rounds:
                    return
            else:
                idle_rounds = 0
            self._stop.wait(poll)

    # ---------- 租约 ----------

    def _claim(self, conn, carrier: str, shard: int = 0,
               shard_count: int = 1) -> list:
        now = to_utc_iso(now_utc())
        lease_end = to_utc_iso(utc_plus(seconds=config.LEASE_SECONDS))
        with transaction(conn):
            if shard_count > 1:
                rows = conn.execute(
                    "SELECT id, tracking_no FROM waybills "
                    "WHERE carrier_code=? AND active=1 AND next_poll_at<=? "
                    "AND (lease_owner IS NULL OR lease_expires_at<?) "
                    "AND id % ? = ? "
                    "ORDER BY next_poll_at LIMIT ?",
                    (carrier, now, now, shard_count, shard, CLAIM_BATCH)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, tracking_no FROM waybills "
                    "WHERE carrier_code=? AND active=1 AND next_poll_at<=? "
                    "AND (lease_owner IS NULL OR lease_expires_at<?) "
                    "ORDER BY next_poll_at LIMIT ?",
                    (carrier, now, now, CLAIM_BATCH)).fetchall()
            if rows:
                ids = [r["id"] for r in rows]
                execute_with_retry(
                    conn,
                    f"UPDATE waybills SET lease_owner=?, lease_expires_at=? "
                    f"WHERE id IN ({','.join('?' * len(ids))})",
                    (self.instance_id, lease_end, *ids))
            return rows

    # ---------- worker ----------

    def _worker_loop(self, carrier: str, shard: int = 0,
                     shard_count: int = 1) -> None:
        worker_name = threading.current_thread().name
        conn = get_conn()
        cfg = self._load_config(conn, carrier)
        fetcher = self._build_fetcher(carrier, cfg)
        try:
            fetcher.start()
            while not self._stop.is_set():
                if self._circuit_open(conn, carrier):
                    self._stop.wait(config.scale(5))
                    continue
                rows = self._claim(conn, carrier, shard, shard_count)
                if not rows:
                    self._stop.wait(config.scale(2))
                    continue
                for row in rows:
                    if self._stop.is_set():
                        break
                    self._poll_one(conn, carrier, cfg, fetcher, row, worker_name)
                    cfg = self._load_config(conn, carrier)  # 配置可被 Web 端热改
        except Exception:
            log.exception("[%s] worker 线程异常退出", carrier)
        finally:
            fetcher.close()
            conn.close()

    def _load_config(self, conn, carrier: str) -> dict:
        row = conn.execute(
            "SELECT * FROM carrier_configs WHERE code=?", (carrier,)).fetchone()
        return dict(row) if row else {}

    def _build_fetcher(self, carrier: str, cfg: dict) -> CarrierFetcher:
        params = json.loads(cfg.get("params_json") or "{}")
        return CarrierFetcher(
            carrier,
            base_url=cfg["base_url"].rsplit("/", 1)[0],  # 去掉 /carrier 后缀
            mode=cfg["mode"],
            needs_login=bool(cfg["needs_login"]),
            login_user=cfg.get("login_user"),
            login_pass=cfg.get("login_pass"),
            rate_rps=float(cfg["rate_rps"]),
            sign_secret=params.get("sign_secret"),
        )

    # ---------- 熔断 ----------

    def _circuit_open(self, conn, carrier: str) -> bool:
        row = conn.execute(
            "SELECT circuit_open_until, consecutive_fails FROM carrier_configs "
            "WHERE code=?", (carrier,)).fetchone()
        if not row or not row["circuit_open_until"]:
            return False
        until = parse_iso(row["circuit_open_until"])
        if now_utc() < until:
            return True
        # 半开探测：清空状态放行一次
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET circuit_open_until=NULL WHERE code=?",
            (carrier,))
        return False

    def _record_success(self, conn, carrier: str) -> None:
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET consecutive_fails=0, circuit_open_until=NULL "
            "WHERE code=?", (carrier,))
        resolve_alert(conn, "site_down", carrier_code=carrier)

    def _record_failure(self, conn, carrier: str, kind: str, message: str) -> None:
        # 原子自增，避免多分片并发读-改-写丢失计数
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET consecutive_fails=consecutive_fails+1 "
            "WHERE code=?", (carrier,))
        row = conn.execute(
            "SELECT consecutive_fails FROM carrier_configs WHERE code=?",
            (carrier,)).fetchone()
        fails = row["consecutive_fails"] if row else 1
        open_until = None
        if fails >= config.CIRCUIT_FAIL_THRESHOLD:
            open_until = to_utc_iso(utc_plus(seconds=config.CIRCUIT_OPEN_SECONDS))
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET circuit_open_until=? WHERE code=?",
            (open_until, carrier))
        if kind in ("http_error", "timeout", "rate_limited"):
            raise_alert(conn, "site_down" if fails >= config.CIRCUIT_FAIL_THRESHOLD
                        else kind,
                        f"{carrier} 连续失败 {fails} 次：{message}",
                        carrier_code=carrier)
        elif kind == "login_failed":
            raise_alert(conn, "login_failed",
                        f"{carrier} 登录/鉴权失败：{message}", carrier_code=carrier)

    # ---------- 单运单一次完整轮询 ----------

    def _poll_one(self, conn, carrier: str, cfg: dict,
                  fetcher: CarrierFetcher, row, worker_name: str) -> None:
        waybill_id = row["id"]
        tracking_no = row["tracking_no"]
        started = to_utc_iso(now_utc())
        log.info("[%s] poll %s", carrier, tracking_no)
        result = fetcher.fetch(tracking_no)
        finished = to_utc_iso(now_utc())

        if not result.ok:
            self._handle_fetch_failure(conn, carrier, waybill_id, cfg, result,
                                       started, finished, worker_name)
            return

        # 解析（HTML 模式 payload 是 Playwright Page）
        page = result.payload
        try:
            nodes = parse_payload(carrier, page, cfg.get("parser_version", "v1"))
        except ParseError as e:
            self._handle_parse_error(conn, carrier, waybill_id, cfg, str(e),
                                     result.http_status, started, finished,
                                     worker_name)
            return
        except Exception as e:  # 未知解析异常同样视为改版/故障，禁止吞掉
            self._handle_parse_error(conn, carrier, waybill_id, cfg,
                                     f"unexpected: {e}", result.http_status,
                                     started, finished, worker_name)
            return
        finally:
            if hasattr(page, "close"):
                try:
                    page.close()
                except Exception:
                    pass

        if not nodes:
            self._handle_parse_error(conn, carrier, waybill_id, cfg,
                                     "解析结果为空，拒绝写入空轨迹",
                                     result.http_status, started, finished,
                                     worker_name)
            return

        snapshot_mode = carrier == "eta"
        with transaction(conn):
            merge = merge_nodes(conn, waybill_id, carrier, nodes,
                                snapshot_mode=snapshot_mode)
            self._record_success(conn, carrier)
            next_poll = compute_next_poll(
                conn, waybill_id, json.loads(cfg.get("poll_overrides") or "{}"),
                failed=False, failure_count=0)
            execute_with_retry(
                conn,
                "UPDATE waybills SET last_polled_at=?, last_success_at=?, "
                "poll_count=poll_count+1, fail_count=0, next_poll_at=?, "
                "lease_owner=NULL, lease_expires_at=NULL WHERE id=?",
                (finished, finished, next_poll, waybill_id))
            self._write_log(conn, waybill_id, carrier, started, finished, True,
                            result.http_status, len(nodes), merge.added,
                            merge.retracted, None, "", result.attempts)
            for uid in merge.unknown_statuses:
                raise_alert(conn, "unknown_status",
                            f"{carrier} 运单 {tracking_no} 节点 {uid} 状态无法映射",
                            carrier_code=carrier, waybill_id=waybill_id)
        with self._stats_lock:
            s = self.stats.setdefault(worker_name, _empty_stat())
            s["polls"] += 1
            s["ok"] += 1
            s["added"] += merge.added

    def _handle_fetch_failure(self, conn, carrier: str, waybill_id: int,
                              cfg: dict, result, started: str, finished: str,
                              worker_name: str) -> None:
        kind = result.error_kind or "http_error"
        with transaction(conn):
            self._record_failure(conn, carrier, kind, result.error_message)
            row = conn.execute(
                "SELECT fail_count FROM waybills WHERE id=?", (waybill_id,)
            ).fetchone()
            fail_count = (row["fail_count"] if row else 0) + 1
            next_poll = compute_next_poll(
                conn, waybill_id, json.loads(cfg.get("poll_overrides") or "{}"),
                failed=True, failure_count=fail_count)
            execute_with_retry(
                conn,
                "UPDATE waybills SET last_polled_at=?, poll_count=poll_count+1, "
                "fail_count=?, next_poll_at=?, lease_owner=NULL, "
                "lease_expires_at=NULL WHERE id=?",
                (finished, fail_count, next_poll, waybill_id))
            self._write_log(conn, waybill_id, carrier, started, finished, False,
                            result.http_status, 0, 0, 0, kind,
                            result.error_message, result.attempts)
            if kind == "unknown_waybill":
                tracking = conn.execute(
                    "SELECT tracking_no FROM waybills WHERE id=?", (waybill_id,)
                ).fetchone()
                raise_alert(conn, "unknown_waybill",
                            f"{carrier} 单号 {tracking['tracking_no'] if tracking else '?'} "
                            f"在承运商系统不存在",
                            carrier_code=carrier, waybill_id=waybill_id)
        with self._stats_lock:
            s = self.stats.setdefault(worker_name, _empty_stat())
            s["polls"] += 1
            s["failed"] += 1
            s["last_error"] = f"{kind}: {result.error_message}"

    def _handle_parse_error(self, conn, carrier: str, waybill_id: int, cfg: dict,
                            message: str, http_status, started: str,
                            finished: str, worker_name: str) -> None:
        # 关键要求：改版/解析失败必须告警，且不能写入空数据
        with transaction(conn):
            raise_alert(conn, "parse_error",
                        f"{carrier} 解析失败（疑似页面改版）：{message}",
                        carrier_code=carrier, waybill_id=waybill_id)
            row = conn.execute(
                "SELECT fail_count FROM waybills WHERE id=?", (waybill_id,)
            ).fetchone()
            fail_count = (row["fail_count"] if row else 0) + 1
            next_poll = compute_next_poll(
                conn, waybill_id, json.loads(cfg.get("poll_overrides") or "{}"),
                failed=True, failure_count=fail_count)
            execute_with_retry(
                conn,
                "UPDATE waybills SET last_polled_at=?, poll_count=poll_count+1, "
                "fail_count=?, next_poll_at=?, lease_owner=NULL, "
                "lease_expires_at=NULL WHERE id=?",
                (finished, fail_count, next_poll, waybill_id))
            self._write_log(conn, waybill_id, carrier, started, finished, False,
                            http_status, 0, 0, 0, "parse_error", message, 1)
        with self._stats_lock:
            s = self.stats.setdefault(worker_name, _empty_stat())
            s["polls"] += 1
            s["failed"] += 1
            s["last_error"] = f"parse_error: {message}"

    @staticmethod
    def _write_log(conn, waybill_id: int, carrier: str, started: str, finished: str,
                   ok: bool, http_status, nodes_seen: int, nodes_added: int,
                   nodes_retracted: int, error_kind: str | None,
                   error_message: str, attempts: int) -> None:
        execute_with_retry(
            conn,
            "INSERT INTO fetch_logs (waybill_id,carrier_code,started_at,finished_at,"
            "ok,http_status,nodes_seen,nodes_added,nodes_retracted,error_kind,"
            "error_message,attempt_no) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (waybill_id, carrier, started, finished, 1 if ok else 0, http_status,
             nodes_seen, nodes_added, nodes_retracted, error_kind,
             error_message, attempts))

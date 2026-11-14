"""抓取层：把“取原始数据”和“解析/入库”分开。

CarrierFetcher 在承运商 worker 线程内启动，全程复用一个 Chromium 实例：
- browser_page   ：Playwright 打开 HTML（beta 自动登录并维持会话）
- browser_json   ：Playwright 打开页面取 token，浏览器上下文内调 JSON
- request_json   ：requests 直连 JSON
- request_signed ：requests + HMAC 签名

传输类错误（429/5xx/超时）在此指数退避重试；解析错误不重试，交给 worker 告警。
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from playwright.sync_api import Page, sync_playwright

from . import config
from .carriers import CARRIERS

log = logging.getLogger("collector.fetcher")


def _max_attempts() -> int:
    # 动态读取，测试可通过环境变量在运行期调小；生产默认 5 次传输重试
    return int(__import__("os").environ.get("COLLECTOR_FETCH_ATTEMPTS", "5"))


@dataclass
class FetchResult:
    ok: bool
    payload: object = None          # HTML 页面对应 Page，JSON 对应 dict
    http_status: int | None = None
    error_kind: str | None = None
    error_message: str = ""
    attempts: int = 1
    retry_after: float = 0.0


class _Pacer:
    """客户端令牌桶，按承运商 rps 平滑发请求，尽量不触发站点 429。"""

    def __init__(self, rps: float) -> None:
        self.min_gap = 1.0 / max(0.1, rps)
        self._last = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            sleep_for = self._last + self.min_gap - now
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._last = time.monotonic()


class CarrierFetcher:
    def __init__(self, carrier_code: str, base_url: str, mode: str, *,
                 needs_login: bool = False, login_user: str | None = None,
                 login_pass: str | None = None, rate_rps: float = 2.0,
                 sign_secret: str | None = None) -> None:
        self.code = carrier_code
        spec = CARRIERS[carrier_code]
        self.base_url = base_url.rstrip("/")
        self.mode = mode
        self.needs_login = needs_login
        self.login_user = login_user
        self.login_pass = login_pass
        self.pacer = _Pacer(rate_rps)
        self.sign_secret = sign_secret
        self._pw = None
        self._browser = None
        self._context = None
        self._http = requests.Session()
        self._beta_logged_in = False

    # ---------- 生命周期 ----------

    def start(self) -> None:
        if self.mode in ("browser_page", "browser_json"):
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(headless=True)
            self._context = self._browser.new_context()

    def close(self) -> None:
        try:
            if self._context:
                self._context.close()
            if self._browser:
                self._browser.close()
        finally:
            if self._pw:
                self._pw.stop()
            self._http.close()

    def _new_page(self) -> Page:
        assert self._context is not None
        return self._context.new_page()

    # ---------- 对外入口（带退避重试） ----------

    def fetch(self, tracking_no: str) -> FetchResult:
        last: FetchResult | None = None
        for attempt in range(1, _max_attempts() + 1):
            self.pacer.wait()
            result = self._fetch_once(tracking_no)
            result.attempts = attempt
            if result.ok:
                return result
            last = result
            # 404 单号不存在、401 签名/登录失败：重试无意义
            if result.error_kind in ("unknown_waybill", "login_failed"):
                return result
            if attempt >= _max_attempts():
                break
            backoff = min(config.MAX_REMINDER_BACKOFF,
                          (2 ** attempt) * config.scale(1))
            delay = max(result.retry_after, backoff)
            log.warning("[%s] %s 第 %d 次抓取失败(%s)，%.1fs 后重试",
                        self.code, tracking_no, attempt, result.error_kind, delay)
            time.sleep(delay)
        assert last is not None
        return last

    # ---------- 各模式实现 ----------

    def _fetch_once(self, tracking_no: str) -> FetchResult:
        try:
            if self.mode == "browser_page":
                return self._fetch_page(tracking_no)
            if self.mode == "browser_json":
                return self._fetch_browser_json(tracking_no)
            if self.mode == "request_json":
                return self._fetch_http_json(tracking_no)
            if self.mode == "request_signed":
                return self._fetch_signed(tracking_no)
            return FetchResult(False, error_kind="config_error",
                               error_message=f"unknown mode {self.mode}")
        except requests.Timeout:
            return FetchResult(False, error_kind="timeout", error_message="request timeout")
        except requests.RequestException as e:
            return FetchResult(False, error_kind="http_error", error_message=str(e))
        except Exception as e:  # Playwright 错误等
            msg = str(e).lower()
            kind = "timeout" if "timeout" in msg else "http_error"
            return FetchResult(False, error_kind=kind, error_message=str(e))

    def _classify_http(self, status: int, body_text: str = "") -> FetchResult | None:
        if status == 200:
            return None
        if status == 404:
            return FetchResult(False, http_status=404, error_kind="unknown_waybill",
                               error_message=body_text[:200])
        if status == 429:
            return FetchResult(False, http_status=429, error_kind="rate_limited",
                               error_message="429 Too Many Requests", retry_after=5)
        if status in (401, 403):
            return FetchResult(False, http_status=status, error_kind="login_failed",
                               error_message=body_text[:200])
        return FetchResult(False, http_status=status, error_kind="http_error",
                           error_message=body_text[:200])

    def _fetch_page(self, tracking_no: str) -> FetchResult:
        page = self._new_page()
        try:
            url = f"{self.base_url}/{self.code}/track?no={tracking_no}"
            resp = page.goto(url, wait_until="networkidle", timeout=20000)
            status = resp.status if resp else 500
            # beta 未登录会被重定向到登录页
            if self.code == "beta" and "/beta/login" in page.url:
                login_ok = self._do_beta_login(page)
                if not login_ok:
                    return FetchResult(False, http_status=401,
                                       error_kind="login_failed",
                                       error_message="beta 登录失败")
                resp = page.goto(url, wait_until="networkidle", timeout=20000)
                status = resp.status if resp else 500
            if status != 200:
                body = page.content()[:200]
                err = self._classify_http(status, body)
                return err or FetchResult(False, http_status=status,
                                          error_kind="http_error")
            # 成功时把 page 交给 worker 解析，由 worker 负责 close
            stolen = page
            page = None  # type: ignore[assignment]
            return FetchResult(True, payload=stolen, http_status=200)
        finally:
            if page is not None:
                page.close()

    def _do_beta_login(self, page: Page) -> bool:
        try:
            page.wait_for_selector("input[name='username']", timeout=8000)
            page.fill("input[name='username']", self.login_user or "")
            page.fill("input[name='password']", self.login_pass or "")
            page.click("button")
            page.wait_for_load_state("networkidle", timeout=15000)
            self._beta_logged_in = "/beta/login" not in page.url
            return self._beta_logged_in
        except Exception as e:
            log.warning("[beta] 登录异常: %s", e)
            return False

    def _fetch_browser_json(self, tracking_no: str) -> FetchResult:
        page = self._new_page()
        try:
            portal = f"{self.base_url}/zeta/portal?no={tracking_no}"
            resp = page.goto(portal, wait_until="networkidle", timeout=20000)
            if not resp or resp.status != 200:
                status = resp.status if resp else 500
                return self._classify_http(status) or FetchResult(
                    False, http_status=status, error_kind="http_error")
            app = page.locator("#zeta-app")
            token = app.get_attribute("data-token")
            if not token:
                return FetchResult(False, error_kind="login_failed",
                                   error_message="zeta 页面未下发 token")
            api_url = (f"{self.base_url}/zeta/api/trace?"
                       + urlencode({"no": tracking_no, "token": token}))
            api_resp = page.request.get(api_url, timeout=20000)
            if api_resp.status != 200:
                return self._classify_http(api_resp.status, api_resp.text()[:200]) \
                    or FetchResult(False, http_status=api_resp.status,
                                   error_kind="http_error")
            return FetchResult(True, payload=api_resp.json(),
                               http_status=200)
        finally:
            page.close()

    def _fetch_http_json(self, tracking_no: str) -> FetchResult:
        if self.code == "delta":
            url = f"{self.base_url}/delta/api/eventos"
            params = {"numero": tracking_no}
        else:  # eta
            url = f"{self.base_url}/eta/api/snapshot"
            params = {"consignment": tracking_no}
        resp = self._http.get(url, params=params, timeout=15)
        err = self._classify_http(resp.status_code, resp.text[:200])
        if err:
            if err.error_kind == "rate_limited":
                err.retry_after = float(resp.headers.get("Retry-After", 5))
            return err
        return FetchResult(True, payload=resp.json(), http_status=200)

    def _fetch_signed(self, tracking_no: str) -> FetchResult:
        import hashlib
        import hmac

        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        nonce = uuid.uuid4().hex
        sign = hmac.new(
            (self.sign_secret or "").encode(),
            f"{tracking_no}|{ts}|{nonce}".encode(),
            hashlib.sha256,
        ).hexdigest()
        url = f"{self.base_url}/gamma/api/v1/trace"
        params = {"tracking_no": tracking_no, "ts": ts,
                  "nonce": nonce, "sign": sign}
        resp = self._http.get(url, params=params, timeout=15)
        err = self._classify_http(resp.status_code, resp.text[:200])
        if err:
            if err.error_kind == "rate_limited":
                err.retry_after = float(resp.headers.get("Retry-After", 5))
            return err
        return FetchResult(True, payload=resp.json(), http_status=200)

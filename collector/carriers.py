"""承运商注册表：单号识别规则、接入模式、凭据/密钥等单一事实来源。

仿真站 sim/ 与采集端 collector/ 都从这里取规则，避免两边规则漂移。
mode 取值：
  browser_page   —— Playwright 打开 HTML 页面，从 DOM 解析
  browser_json   —— Playwright 打开页面拿 token，再在浏览器上下文内请求 JSON
  request_json   —— requests 直接请求 JSON 接口（带限流）
  request_signed —— requests 请求带签名的 JSON 接口
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class CarrierSpec:
    code: str
    name: str
    number_pattern: str
    mode: str
    needs_login: bool = False
    login_user: str | None = None
    login_pass: str | None = None
    params: dict = field(default_factory=dict)
    rate_rps: float = 2.0
    # 解析器支持的页面版本；theta 改版后需要把 parser_version 切到 v2
    parser_versions: tuple[str, ...] = ("v1",)


CARRIERS: dict[str, CarrierSpec] = {
    "alpha": CarrierSpec(
        code="alpha", name="Alpha 速运（HTML 表格）",
        number_pattern=r"^YP\d{10}$", mode="browser_page", rate_rps=3.0,
    ),
    "beta": CarrierSpec(
        code="beta", name="Beta Parcels（登录后 HTML）",
        number_pattern=r"^UK\d{9}$", mode="browser_page", needs_login=True,
        login_user="csuser", login_pass="cs2026", rate_rps=2.0,
    ),
    "gamma": CarrierSpec(
        code="gamma", name="Gamma 签名字 API",
        number_pattern=r"^GMA-\d{8}$", mode="request_signed",
        params={"sign_secret": "gamma-secret-7f3a"}, rate_rps=4.0,
    ),
    "delta": CarrierSpec(
        code="delta", name="Delta Logística（限流 JSON）",
        number_pattern=r"^BR\d{11}BR$", mode="request_json", rate_rps=1.0,
    ),
    "epsilon": CarrierSpec(
        code="epsilon", name="Epsilon 運輸（日文 HTML）",
        number_pattern=r"^JP\d{12}JP$", mode="browser_page", rate_rps=2.0,
    ),
    "zeta": CarrierSpec(
        code="zeta", name="Zeta EU（页内 token + JSON）",
        number_pattern=r"^Z\d{10}EU$", mode="browser_json", rate_rps=2.0,
    ),
    "eta": CarrierSpec(
        code="eta", name="Eta Post（快照 JSON，支持节点撤回）",
        number_pattern=r"^EE[0-9A-F]{9}$", mode="request_json", rate_rps=3.0,
    ),
    "theta": CarrierSpec(
        code="theta", name="Theta Mail（会改版的 HTML）",
        number_pattern=r"^TH\d{12}$", mode="browser_page", rate_rps=2.0,
        parser_versions=("v1", "v2"),
    ),
}


def recognize(tracking_no: str) -> str | None:
    """按单号规则识别承运商，识别不了返回 None。"""
    for code, spec in CARRIERS.items():
        if re.match(spec.number_pattern, tracking_no):
            return code
    return None

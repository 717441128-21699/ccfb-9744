"""统一状态码 / 时区 / 节点模型。

各家承运商的原始状态文案与状态码都不同，解析器负责映射到下面这套
系统内部唯一的规范状态码（canonical status）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from zoneinfo import ZoneInfo


class Status(str, Enum):
    PENDING = "PENDING"            # 已预报 / 尚未揽收
    PICKED_UP = "PICKED_UP"        # 已揽收
    IN_TRANSIT = "IN_TRANSIT"      # 运输中
    CUSTOMS = "CUSTOMS"            # 清关中
    RETURNED = "RETURNED"          # 退回
    EXCEPTION = "EXCEPTION"        # 异常 / 滞留
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERED = "DELIVERED"        # 已签收（终态，不再查询）
    UNKNOWN = "UNKNOWN"            # 无法映射（不应静默出现，需告警）


# 终态：进入后不再调度查询
TERMINAL_STATUSES = frozenset({Status.DELIVERED, Status.RETURNED})


# 每家承运商的原始状态 -> 规范状态。
# 故意让各家的原始文案各不相同，模拟真实世界。
CARRIER_STATUS_MAP: dict[str, dict[str, Status]] = {
    "alpha": {
        "ORDERED": Status.PENDING,
        "COLLECTED": Status.PICKED_UP,
        "ON_THE_WAY": Status.IN_TRANSIT,
        "CUSTOMS_HOLD": Status.CUSTOMS,
        "DELIVERING": Status.OUT_FOR_DELIVERY,
        "SIGNED": Status.DELIVERED,
    },
    "beta": {  # 英文文案
        "Manifested": Status.PENDING,
        "Picked up": Status.PICKED_UP,
        "In transit": Status.IN_TRANSIT,
        "Customs clearance": Status.CUSTOMS,
        "Out for delivery": Status.OUT_FOR_DELIVERY,
        "Delivered": Status.DELIVERED,
        "Return to sender": Status.RETURNED,
    },
    "gamma": {  # 数字码
        "100": Status.PENDING,
        "200": Status.PICKED_UP,
        "300": Status.IN_TRANSIT,
        "400": Status.CUSTOMS,
        "700": Status.OUT_FOR_DELIVERY,
        "900": Status.DELIVERED,
        "800": Status.EXCEPTION,
    },
    "delta": {
        "EMITIDO": Status.PENDING,
        "COLETADO": Status.PICKED_UP,
        "EM_TRANSITO": Status.IN_TRANSIT,
        "LIBERACAO_ADUANEIRA": Status.CUSTOMS,
        "SAIU_ENTREGA": Status.OUT_FOR_DELIVERY,
        "ENTREGUE": Status.DELIVERED,
    },
    "epsilon": {  # 日文
        "登録済": Status.PENDING,
        "集荷済": Status.PICKED_UP,
        "輸送中": Status.IN_TRANSIT,
        "通関中": Status.CUSTOMS,
        "配達中": Status.OUT_FOR_DELIVERY,
        "配達完了": Status.DELIVERED,
        "差戻": Status.RETURNED,
    },
    "zeta": {
        "CREATED": Status.PENDING,
        "PICKUP": Status.PICKED_UP,
        "TRANSIT": Status.IN_TRANSIT,
        "CUSTOMS": Status.CUSTOMS,
        "OFD": Status.OUT_FOR_DELIVERY,
        "POD": Status.DELIVERED,
    },
    "eta": {
        "booked": Status.PENDING,
        "collected": Status.PICKED_UP,
        "moving": Status.IN_TRANSIT,
        "customs": Status.CUSTOMS,
        "delivery": Status.OUT_FOR_DELIVERY,
        "closed": Status.DELIVERED,
    },
    "theta": {
        "ACCEPTED": Status.PENDING,
        "RETRIEVED": Status.PICKED_UP,
        "ROUTING": Status.IN_TRANSIT,
        "BONDED": Status.CUSTOMS,
        "DISPATCHED": Status.OUT_FOR_DELIVERY,
        "COMPLETE": Status.DELIVERED,
    },
}


# 各家展示时间所用时区（仿真站按此时区渲染本地时间，解析器须转回 UTC）。
CARRIER_TIMEZONES: dict[str, str] = {
    "alpha": "Asia/Shanghai",        # UTC+8
    "beta": "Europe/London",         # 11 月为 UTC+0
    "gamma": "America/Los_Angeles",  # 11 月 PST UTC-8
    "delta": "America/Sao_Paulo",    # UTC-3
    "epsilon": "Asia/Tokyo",         # UTC+9
    "zeta": "Europe/Berlin",         # 11 月 CET UTC+1
    "eta": "Asia/Dubai",             # UTC+4
    "theta": "America/New_York",     # 11 月 EST UTC-5
}


def map_status(carrier: str, raw_status: str) -> Status:
    """把承运商原始状态映射为规范状态；映射不了就返回 UNKNOWN（调用方须告警）。"""
    table = CARRIER_STATUS_MAP.get(carrier, {})
    return table.get(raw_status, Status.UNKNOWN)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_utc_iso(dt: datetime) -> str:
    """统一以 UTC、带偏移量的 ISO8601 字符串入库。"""
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_local(carrier: str, local_text: str, fmt: str) -> datetime:
    """把承运商页面上的本地时间字符串解析为带时区的 datetime（UTC 换算由 to_utc_iso 做）。"""
    tz = ZoneInfo(CARRIER_TIMEZONES[carrier])
    naive = datetime.strptime(local_text, fmt)
    return naive.replace(tzinfo=tz)


@dataclass(frozen=True)
class CanonicalNode:
    """统一节点格式。uid 是同一节点的幂等身份，用于去重/撤回对账。"""
    uid: str
    seq: int
    event_time_utc: str
    status: str
    description: str
    location: str
    retracted: bool = False

"""全局配置：路径、仿真站地址、调度参数。

时间尺度可以通过环境变量 COLLECTOR_TIME_SCALE 调小（默认 1.0），
让端到端测试在几分钟内跑完而不是等几个小时。
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("COLLECTOR_DATA_DIR", ROOT / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "collector.db"
SIM_DB_PATH = DATA_DIR / "simulator.db"

SIM_HOST = os.environ.get("SIM_HOST", "127.0.0.1")
SIM_PORT = int(os.environ.get("SIM_PORT", "8765"))
SIM_BASE = f"http://{SIM_HOST}:{SIM_PORT}"

WEB_HOST = os.environ.get("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.environ.get("WEB_PORT", "8800"))

# 测试加速：所有“秒级”调度间隔乘以此系数
TIME_SCALE = float(os.environ.get("COLLECTOR_TIME_SCALE", "1.0"))

# 运单被 worker 领取后的租约秒数；worker 崩溃后超过租约即可被重新领取
LEASE_SECONDS = int(120 * TIME_SCALE)

# 熔断：连续失败 N 次后断开 OPEN_SECONDS，期间跳过该承运商
CIRCUIT_FAIL_THRESHOLD = 5
CIRCUIT_OPEN_SECONDS = int(180 * TIME_SCALE)
CIRCUIT_HALF_PROBE = 1

# 限流默认参数（可被 carrier_configs 覆盖）
DEFAULT_RATE_LIMIT_RPS = 2.0

# 告警冷却：同类告警在该秒数内不重复刷屏
ALERT_COOLDOWN_SECONDS = int(300 * TIME_SCALE)

# 按规范状态决定的基础查询间隔（秒）。已签收/退回为终态不查询。
POLL_INTERVALS_SECONDS = {
    "PENDING": int(3600 * TIME_SCALE),
    "PICKED_UP": int(1800 * TIME_SCALE),
    "IN_TRANSIT": int(900 * TIME_SCALE),
    "CUSTOMS": int(14400 * TIME_SCALE),
    "OUT_FOR_DELIVERY": int(600 * TIME_SCALE),
    "EXCEPTION": int(2700 * TIME_SCALE),
    "UNKNOWN": int(7200 * TIME_SCALE),
}

# 新运单首次查询、以及重试时的短间隔
FAST_POLL_SECONDS = int(30 * TIME_SCALE)
MAX_REMINDER_BACKOFF = int(1800 * TIME_SCALE)

# 终态（签收/退回）宽限复核：进入终态后仍复核 N 次，
# 用于捕获“事后撤回/迟到节点”这类真实场景；之后才真正停止查询
TERMINAL_GRACE_POLLS = 3
TERMINAL_GRACE_INTERVAL = int(120 * TIME_SCALE)

# 每家承运商的并发 worker 分片数。慢站点（低 rps、易超时）多分片，
# 分片之间用 waybill.id 取模分工，租约保证同一单不会被并发抓取。
CARRIER_WORKERS = {
    "delta": 4,   # 1 rps 严格限流，单线程会被退避拖死
    "beta": 2,    # 需登录 + 浏览器，稍慢
    "theta": 2,
}


def scale(seconds: float) -> int:
    return max(1, int(seconds * TIME_SCALE))

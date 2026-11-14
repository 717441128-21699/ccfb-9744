"""测试夹具：隔离数据目录 + 加速时间尺度 + 后台活体仿真站。"""
from __future__ import annotations

import os
import socket
import sys
import threading
import time
from pathlib import Path

import pytest
import requests
from werkzeug.serving import make_server

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _free_port(avoid: set[int] | None = None) -> int:
    avoid = avoid or set()
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        if port not in avoid:
            return port


@pytest.fixture(scope="session")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("collector-test")

    from collector import config
    # config 在收集阶段就可能已被导入，环境变量改不动已加载的常量，
    # 因此直接覆盖模块属性，并同步环境变量供子进程（恢复测试）读取。
    config.DATA_DIR = tmp
    config.DB_PATH = tmp / "collector.db"
    config.SIM_DB_PATH = tmp / "simulator.db"
    config.TIME_SCALE = 0.02
    config.LEASE_SECONDS = 5
    config.CIRCUIT_OPEN_SECONDS = 5
    config.CIRCUIT_FAIL_THRESHOLD = 2
    config.ALERT_COOLDOWN_SECONDS = 2
    # 失败退避保持秒级，避免退避风暴又能较快重试
    config.FAST_POLL_SECONDS = 2
    config.MAX_REMINDER_BACKOFF = 8
    # 测试环境把在途轮询压到 3~6 秒，保证 worker 能追上“分批可见”的节点
    test_intervals = {
        "PENDING": 4, "PICKED_UP": 3, "IN_TRANSIT": 3, "CUSTOMS": 6,
        "OUT_FOR_DELIVERY": 3, "EXCEPTION": 4, "UNKNOWN": 6,
    }
    for k, v in test_intervals.items():
        config.POLL_INTERVALS_SECONDS[k] = v
    # 终态宽限复核在测试里缩短
    config.TERMINAL_GRACE_POLLS = 2
    config.TERMINAL_GRACE_INTERVAL = 3
    config.SIM_PORT = _free_port()
    config.WEB_PORT = _free_port(avoid={config.SIM_PORT})
    os.environ["COLLECTOR_DATA_DIR"] = str(tmp)
    os.environ["COLLECTOR_TIME_SCALE"] = "0.02"
    os.environ["COLLECTOR_FETCH_ATTEMPTS"] = "1"
    os.environ["SIM_PORT"] = str(config.SIM_PORT)
    os.environ["WEB_PORT"] = str(config.WEB_PORT)

    from sim.generate import generate as generate_data
    from sim.app import app as sim_app

    stats = generate_data(per_carrier=12)

    from collector.db import init_db, get_conn
    from collector.bootstrap import bootstrap_carriers
    init_db()
    _conn = get_conn()
    try:
        bootstrap_carriers(_conn, f"http://{config.SIM_HOST}:{config.SIM_PORT}")
    finally:
        _conn.close()

    server = make_server(config.SIM_HOST, config.SIM_PORT, sim_app, threaded=True)
    started = threading.Event()
    boot_error: list[BaseException] = []

    def _serve():
        try:
            started.set()
            server.serve_forever()
        except BaseException as e:  # noqa: BLE001 - 测试夹具需要把异常带回主线程
            boot_error.append(e)

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
    if boot_error:
        raise boot_error[0]
    base = f"http://{config.SIM_HOST}:{config.SIM_PORT}"
    import traceback
    deadline = time.time() + 15
    last_err = ""
    while time.time() < deadline:
        try:
            if requests.get(base + "/health", timeout=2,
                            proxies={"http": None, "https": None}).ok:
                break
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(0.2)
    else:
        raise RuntimeError(
            f"仿真站启动超时; thread_alive={t.is_alive()} boot_error={boot_error} "
            f"last_err={last_err} port={config.SIM_PORT}")

    yield {"config": config, "stats": stats, "base": base, "tmp": tmp}
    server.shutdown()
    t.join(timeout=5)


def threading_args(server):  # 兼容保留，已不再使用
    import threading as _t
    t = _t.Thread(target=server.serve_forever, daemon=True)
    t.start()
    return t

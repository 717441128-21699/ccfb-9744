"""启动采集 worker（调度引擎）。可多实例同时跑，租约保证不重复查。"""
from __future__ import annotations

import logging
import signal
import time

from collector.bootstrap import bootstrap_carriers
from collector import config
from collector.db import get_conn, init_db
from collector.scheduler import SchedulerEngine


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )
    init_db()
    conn = get_conn()
    try:
        bootstrap_carriers(conn, config.SIM_BASE)
    finally:
        conn.close()

    engine = SchedulerEngine()
    engine.start()

    def _shutdown(_sig, _frame):
        engine.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    try:
        while any(t.is_alive() for t in engine._threads.values()):
            time.sleep(1)
    finally:
        engine.stop()


if __name__ == "__main__":
    main()

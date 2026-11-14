"""启动仿真物流站（8 家承运商，127.0.0.1:8765）。"""
from __future__ import annotations

from sim.app import app
from collector import config
from sim.generate import generate


def main() -> None:
    import os
    if not os.path.exists(config.SIM_DB_PATH):
        stats = generate()
        print("仿真数据已生成:", stats)
    app.run(host=config.SIM_HOST, port=config.SIM_PORT, threaded=True)


if __name__ == "__main__":
    main()

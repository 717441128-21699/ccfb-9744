"""Web 控制台冒烟：五个页面可打开、导入接口可用。"""
from __future__ import annotations

from werkzeug.serving import make_server

from collector import config


def test_web_pages_render(env):
    from web.app import create_app
    app = create_app()
    server = make_server(config.WEB_HOST, config.WEB_PORT, app, threaded=True)
    import threading
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        import requests
        base = f"http://{config.WEB_HOST}:{config.WEB_PORT}"
        for path in ["/", "/carriers", "/waybills", "/changes", "/alerts"]:
            r = requests.get(base + path, timeout=10)
            assert r.status_code == 200, path
            assert "跨境小包" in r.text

        # 导入接口：一个好单号 + 一个坏单号
        r = requests.post(base + "/waybills/import",
                          data={"numbers": "YP2600000001\nNOPE-999"}, timeout=10)
        assert r.status_code == 200
        r = requests.get(base + "/waybills", timeout=10)
        assert "YP2600000001" in r.text
        assert "NOPE-999" in r.text  # 坏单号出现在“识别不了”区域
    finally:
        server.shutdown()
        t.join(timeout=5)

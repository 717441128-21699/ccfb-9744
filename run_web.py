"""启动 Web 控制台（127.0.0.1:8800）。"""
from web.app import create_app
from collector import config


if __name__ == "__main__":
    app = create_app()
    app.run(host=config.WEB_HOST, port=config.WEB_PORT, threaded=True)

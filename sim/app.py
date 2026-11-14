"""仿真物流站 Flask 应用。

一个进程承载 8 家承运商，每家路径/协议/页面结构不同。只绑定 127.0.0.1。
故障注入全部通过 /admin/* 控制，默认关闭：
  down            整站 503（单家隔离测试）
  chaos           按概率随机 500
  schema v2       theta 改版
  retract         eta 撤回节点
  unknown_waybill 在真值库里不存在的单号
"""
from __future__ import annotations

import json
import secrets
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, make_response, redirect, request, url_for

from collector import canonical, config
from collector.carriers import CARRIERS
from .generate import advance as generate_advance
from . import renderers, simlib

app = Flask(__name__)
app.secret_key = "sim-only-local-secret"
_sessions: set[str] = set()
_session_lock = threading.Lock()


# ---------- 故障/版本状态 ----------

def _down_codes() -> set[str]:
    with simlib.sim_conn() as conn:
        return set(json.loads(simlib.get_runtime(conn, "down_codes", "[]")))


def _chaos_codes() -> dict:
    with simlib.sim_conn() as conn:
        return json.loads(simlib.get_runtime(conn, "chaos_codes", "{}"))


def _check_faults(code: str):
    """返回 (response, status) 表示故障响应；None 表示放行。"""
    if code in _down_codes():
        return make_response(f"carrier {code} under maintenance", 503)
    chaos = _chaos_codes()
    if code in chaos:
        # 用单号做确定性抖动，避免“完全随机”让对账不可复现
        sno = request.args.get("tracking_no") or request.args.get("no") or ""
        bucket = sum(sno.encode()) % 100
        if bucket < float(chaos[code]) * 100:
            return make_response("internal simulation error", 500)
    return None


def _rate_guard(code: str):
    ok, wait = simlib.RATE_LIMITER.allow(code)
    if not ok:
        resp = make_response(jsonify(error="rate_limited", retry_after=round(wait, 2)), 429)
        resp.headers["Retry-After"] = str(max(1, round(wait)))
        return resp
    return None


# ---------- beta 登录 ----------

@app.get("/beta/login")
def beta_login_form():
    return f"""<!doctype html><html><body>
    <form method="post" action="{url_for('beta_login')}">
      <input name="username"><input name="password" type="password">
      <button>Login</button></form></body></html>"""


@app.post("/beta/login")
def beta_login():
    spec = CARRIERS["beta"]
    if request.form.get("username") == spec.login_user and \
       request.form.get("password") == spec.login_pass:
        token = secrets.token_hex(12)
        with _session_lock:
            _sessions.add(token)
        resp = make_response(redirect("/beta/track?no=" + request.args.get("next", "")))
        resp.set_cookie("beta_session", token, httponly=True)
        return resp
    return make_response("invalid credentials", 401)


def _beta_authed() -> bool:
    token = request.cookies.get("beta_session", "")
    with _session_lock:
        return token in _sessions


# ---------- HTML 页面承运商 ----------

@app.get("/alpha/track")
def alpha_track():
    code = "alpha"
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("no", "")
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return make_response("not found", 404)
        return renderers.render_alpha(conn, no)


@app.get("/beta/track")
def beta_track():
    code = "beta"
    if not _beta_authed():
        return redirect(url_for("beta_login_form", next=request.args.get("no", "")))
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("no", "")
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return make_response("consignment unknown", 404)
        return renderers.render_beta(conn, no)


@app.get("/epsilon/track")
def epsilon_track():
    code = "epsilon"
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("no", "")
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return make_response("お問い合わせ番号が見つかりません", 404)
        return renderers.render_epsilon(conn, no)


@app.get("/theta/track")
def theta_track():
    code = "theta"
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("no", "")
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return make_response("not found", 404)
        version = simlib.get_runtime(conn, "schema_version", "v1")
        return renderers.render_theta(conn, no, version)


# ---------- zeta：页面 token + JSON ----------

@app.get("/zeta/portal")
def zeta_portal():
    code = "zeta"
    if (fault := _check_faults(code)) is not None:
        return fault
    no = request.args.get("no", "")
    token = renderers.zeta_page_token(no)
    return f"""<!doctype html><html><body>
    <div id="zeta-app" data-token="{token}" data-no="{no}">loading…</div></body></html>"""


@app.get("/zeta/api/trace")
def zeta_api():
    code = "zeta"
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("no", "")
    if request.args.get("token") != renderers.zeta_page_token(no):
        return make_response(jsonify(error="bad token"), 403)
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return jsonify(error="unknown shipment"), 404
        return jsonify(renderers.build_zeta(conn, no))


# ---------- gamma：签名 JSON ----------

@app.get("/gamma/api/v1/trace")
def gamma_api():
    code = "gamma"
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("tracking_no", "")
    ts = request.args.get("ts", "")
    nonce = request.args.get("nonce", "")
    sign = request.args.get("sign", "")
    if not renderers.verify_gamma(no, ts, nonce, sign):
        return make_response(jsonify(error="invalid signature"), 401)
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return jsonify(error="waybill not found"), 404
        return jsonify(renderers.build_gamma(conn, no))


# ---------- delta / eta：普通 JSON（delta 严格限流） ----------

@app.get("/delta/api/eventos")
def delta_api():
    code = "delta"
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("numero", "")
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return jsonify(erro="conhecimento nao localizado"), 404
        return jsonify(renderers.build_delta(conn, no))


@app.get("/eta/api/snapshot")
def eta_api():
    code = "eta"
    if (fault := _check_faults(code)) is not None:
        return fault
    if (limited := _rate_guard(code)) is not None:
        return limited
    no = request.args.get("consignment", "")
    with simlib.sim_conn() as conn:
        if not simlib.waybill_exists(conn, no):
            return jsonify(error="consignment missing"), 404
        return jsonify(renderers.build_eta(conn, no))


@app.get("/health")
def health():
    return jsonify(ok=True, time=canonical.to_utc_iso(canonical.now_utc()))


# ---------- 管理接口（仅本机测试使用） ----------

@app.post("/admin/down")
def admin_down():
    codes = request.json.get("codes", [])
    with simlib.sim_conn() as conn:
        simlib.set_runtime(conn, "down_codes", json.dumps(codes))
    return jsonify(down_codes=codes)


@app.post("/admin/chaos")
def admin_chaos():
    mapping = request.json.get("codes", {})
    with simlib.sim_conn() as conn:
        simlib.set_runtime(conn, "chaos_codes", json.dumps(mapping))
    return jsonify(chaos_codes=mapping)


@app.post("/admin/schema")
def admin_schema():
    version = request.json.get("version", "v1")
    with simlib.sim_conn() as conn:
        simlib.set_runtime(conn, "schema_version", version)
    return jsonify(schema_version=version)


@app.post("/admin/retract")
def admin_retract():
    """撤回 eta 指定运单的指定节点（默认撤回候选单的第 3 个节点）。"""
    tracking_no = request.json["tracking_no"]
    seq = int(request.json.get("seq", 3))
    with simlib.sim_conn() as conn:
        cur = conn.execute(
            "UPDATE sim_events SET retracted=1 WHERE tracking_no=? AND seq=? AND retracted=0",
            (tracking_no, seq),
        )
        conn.commit()
        if cur.rowcount == 0:
            return jsonify(ok=False, reason="node not found"), 404
    return jsonify(ok=True, tracking_no=tracking_no, seq=seq)


@app.post("/admin/advance")
def admin_advance():
    n = generate_advance(max_new=int(request.json.get("max_new", 40)))
    return jsonify(added=n)


@app.get("/admin/runtime")
def admin_runtime():
    with simlib.sim_conn() as conn:
        keys = ("down_codes", "chaos_codes", "schema_version", "retract_candidates")
        return jsonify({k: simlib.get_runtime(conn, k, "") for k in keys})


def create_app() -> Flask:
    return app


if __name__ == "__main__":
    app.run(host=config.SIM_HOST, port=config.SIM_PORT, threaded=True)

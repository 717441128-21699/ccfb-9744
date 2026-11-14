"""Web 控制台路由：承运商配置、任务调度、运单库、轨迹变更、告警。"""
from __future__ import annotations

import json
import sqlite3

from flask import Flask, jsonify, redirect, render_template, request, url_for

from collector import config
from collector.alerts import resolve_alert
from collector.bootstrap import bootstrap_carriers
from collector.canonical import now_utc, to_utc_iso
from collector.db import execute_with_retry, get_conn, init_db
from collector.importer import import_waybills

app = Flask(__name__)


def db() -> sqlite3.Connection:
    init_db()
    return get_conn()


@app.template_filter("fromjson")
def from_json(value):
    try:
        return json.loads(value or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}


@app.template_filter("localtime")
def localtime(value):
    if not value:
        return "—"
    from datetime import datetime, timezone, timedelta
    dt = datetime.fromisoformat(value).astimezone(timezone(timedelta(hours=8)))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------------- 概览/调度 ----------------

@app.get("/")
def dashboard():
    conn = db()
    try:
        carriers = conn.execute(
            "SELECT * FROM carrier_configs ORDER BY code").fetchall()
        stats = {
            "total": conn.execute("SELECT COUNT(*) c FROM waybills").fetchone()["c"],
            "active": conn.execute(
                "SELECT COUNT(*) c FROM waybills WHERE active=1").fetchone()["c"],
            "delivered": conn.execute(
                "SELECT COUNT(*) c FROM waybills WHERE current_status='DELIVERED'"
            ).fetchone()["c"],
            "due": conn.execute(
                "SELECT COUNT(*) c FROM waybills WHERE active=1 AND next_poll_at<=?",
                (to_utc_iso(now_utc()),)).fetchone()["c"],
            "open_alerts": conn.execute(
                "SELECT COUNT(*) c FROM alerts WHERE resolved_at IS NULL"
            ).fetchone()["c"],
        }
        by_status = conn.execute(
            "SELECT current_status, COUNT(*) c FROM waybills GROUP BY current_status"
        ).fetchall()
        return render_template("dashboard.html", carriers=carriers,
                               stats=stats, by_status=by_status)
    finally:
        conn.close()


# ---------------- 承运商配置 ----------------

@app.get("/carriers")
def carriers_page():
    conn = db()
    try:
        rows = conn.execute("SELECT * FROM carrier_configs ORDER BY code").fetchall()
        return render_template("carriers.html", carriers=rows)
    finally:
        conn.close()


@app.post("/carriers/<code>/toggle")
def carrier_toggle(code):
    conn = db()
    try:
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET enabled=1-enabled, updated_at=? WHERE code=?",
            (to_utc_iso(now_utc()), code))
    finally:
        conn.close()
    return redirect(url_for("carriers_page"))


@app.post("/carriers/<code>/parser")
def carrier_parser(code):
    version = request.form["parser_version"]
    conn = db()
    try:
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET parser_version=?, updated_at=? WHERE code=?",
            (version, to_utc_iso(now_utc()), code))
    finally:
        conn.close()
    return redirect(url_for("carriers_page"))


@app.post("/carriers/<code>/rate")
def carrier_rate(code):
    rps = float(request.form["rate_rps"])
    conn = db()
    try:
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET rate_rps=?, updated_at=? WHERE code=?",
            (rps, to_utc_iso(now_utc()), code))
    finally:
        conn.close()
    return redirect(url_for("carriers_page"))


@app.post("/carriers/<code>/reset-circuit")
def carrier_reset_circuit(code):
    conn = db()
    try:
        execute_with_retry(
            conn,
            "UPDATE carrier_configs SET consecutive_fails=0, circuit_open_until=NULL "
            "WHERE code=?", (code,))
    finally:
        conn.close()
    return redirect(url_for("carriers_page"))


# ---------------- 运单库 ----------------

@app.get("/waybills")
def waybills_page():
    q = request.args.get("q", "").strip()
    carrier = request.args.get("carrier", "").strip()
    status = request.args.get("status", "").strip()
    page_no = max(1, int(request.args.get("page", 1)))
    page_size = 50
    sql = ("SELECT w.*, c.name AS carrier_name FROM waybills w "
           "LEFT JOIN carrier_configs c ON c.code=w.carrier_code WHERE 1=1")
    params: list = []
    if q:
        sql += " AND w.tracking_no LIKE ?"
        params.append(f"%{q}%")
    if carrier:
        sql += " AND w.carrier_code=?"
        params.append(carrier)
    if status:
        sql += " AND w.current_status=?"
        params.append(status)
    conn = db()
    try:
        total = conn.execute(
            sql.replace("SELECT w.*, c.name AS carrier_name", "SELECT COUNT(*) c"),
            params).fetchone()["c"]
        rows = conn.execute(
            sql + " ORDER BY w.id DESC LIMIT ? OFFSET ?",
            (*params, page_size, (page_no - 1) * page_size)).fetchall()
        carrier_rows = conn.execute(
            "SELECT code, name FROM carrier_configs ORDER BY code").fetchall()
        unrec = conn.execute(
            "SELECT * FROM unrecognized_waybills ORDER BY id DESC LIMIT 100").fetchall()
        batches = conn.execute(
            "SELECT * FROM import_batches ORDER BY created_at DESC LIMIT 20").fetchall()
        return render_template(
            "waybills.html", waybills=rows, unrecognized=unrec, batches=batches,
            carriers=carrier_rows, total=total, page_no=page_no,
            page_size=page_size, q=q, carrier=carrier, status=status)
    finally:
        conn.close()


@app.get("/waybills/<int:waybill_id>")
def waybill_detail(waybill_id):
    conn = db()
    try:
        wb = conn.execute(
            "SELECT w.*, c.name AS carrier_name FROM waybills w "
            "LEFT JOIN carrier_configs c ON c.code=w.carrier_code "
            "WHERE w.id=?", (waybill_id,)).fetchone()
        if not wb:
            return "not found", 404
        nodes = conn.execute(
            "SELECT * FROM tracking_nodes WHERE waybill_id=? ORDER BY seq",
            (waybill_id,)).fetchall()
        logs = conn.execute(
            "SELECT * FROM fetch_logs WHERE waybill_id=? ORDER BY id DESC LIMIT 30",
            (waybill_id,)).fetchall()
        return render_template("waybill_detail.html", wb=wb, nodes=nodes, logs=logs)
    finally:
        conn.close()


@app.post("/waybills/import")
def waybills_import():
    text = request.form.get("numbers", "")
    filename = "web-import"
    if request.files.get("file") and request.files["file"].filename:
        f = request.files["file"]
        text = f.read().decode("utf-8-sig")
        filename = f.filename
    result = import_waybills(db(), text, filename)
    return render_template("import_result.html", result=result)


# ---------------- 轨迹变更 ----------------

@app.get("/changes")
def changes_page():
    conn = db()
    try:
        rows = conn.execute(
            "SELECT ch.*, w.tracking_no FROM node_changes ch "
            "JOIN waybills w ON w.id=ch.waybill_id "
            "ORDER BY ch.id DESC LIMIT 300").fetchall()
        return render_template("changes.html", changes=rows)
    finally:
        conn.close()


# ---------------- 告警 ----------------

@app.get("/alerts")
def alerts_page():
    conn = db()
    try:
        rows = conn.execute(
            "SELECT a.*, w.tracking_no FROM alerts a "
            "LEFT JOIN waybills w ON w.id=a.waybill_id "
            "ORDER BY (a.resolved_at IS NULL) DESC, a.id DESC LIMIT 200").fetchall()
        return render_template("alerts.html", alerts=rows)
    finally:
        conn.close()


@app.post("/alerts/<int:alert_id>/resolve")
def alert_resolve(alert_id):
    conn = db()
    try:
        execute_with_retry(
            conn, "UPDATE alerts SET resolved_at=? WHERE id=?",
            (to_utc_iso(now_utc()), alert_id))
    finally:
        conn.close()
    return redirect(url_for("alerts_page"))


# ---------------- JSON API（供页面轮询/外部集成） ----------------

@app.get("/api/overview")
def api_overview():
    conn = db()
    try:
        return jsonify({
            "now": to_utc_iso(now_utc()),
            "waybills": dict(conn.execute(
                "SELECT current_status, COUNT(*) c FROM waybills GROUP BY current_status"
            ).fetchall()),
            "open_alerts": [
                dict(r) for r in conn.execute(
                    "SELECT kind, COUNT(*) c FROM alerts WHERE resolved_at IS NULL "
                    "GROUP BY kind")],
        })
    finally:
        conn.close()


def create_app() -> Flask:
    init_db()
    conn = get_conn()
    try:
        bootstrap_carriers(conn, config.SIM_BASE)
    finally:
        conn.close()
    return app


if __name__ == "__main__":
    create_app().run(host=config.WEB_HOST, port=config.WEB_PORT, threaded=True)

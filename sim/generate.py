"""仿真真值数据生成。

生成结果写入独立的 simulator.db，作为后续逐单对账的 ground truth：
- sim_waybills：运单及最终状态
- sim_events：全部节点（event_time_utc 为真值 UTC 时间），
  visible_at 决定节点何时对查询方可见（模拟轨迹随时间追加），
  retracted=1 表示节点被撤回（仅 eta 使用）。

时间以真实 UTC 为锚点，测试时通过 COLLECTOR_TIME_SCALE 压缩间隔。
"""
from __future__ import annotations

import random
import sqlite3
from datetime import timedelta
from pathlib import Path

from collector import canonical, config
from collector.carriers import CARRIERS

# 每家承运商的状态推进链（取前缀作为在途运单）
LIFECYCLES: dict[str, list[str]] = {
    "alpha": ["ORDERED", "COLLECTED", "ON_THE_WAY", "CUSTOMS_HOLD", "DELIVERING", "SIGNED"],
    "beta": ["Manifested", "Picked up", "In transit", "Customs clearance",
             "Out for delivery", "Delivered"],
    "gamma": ["100", "200", "300", "400", "700", "900"],
    "delta": ["EMITIDO", "COLETADO", "EM_TRANSITO", "LIBERACAO_ADUANEIRA",
              "SAIU_ENTREGA", "ENTREGUE"],
    "epsilon": ["登録済", "集荷済", "輸送中", "通関中", "配達中", "配達完了"],
    "zeta": ["CREATED", "PICKUP", "TRANSIT", "CUSTOMS", "OFD", "POD"],
    "eta": ["booked", "collected", "moving", "customs", "delivery", "closed"],
    "theta": ["ACCEPTED", "RETRIEVED", "ROUTING", "BONDED", "DISPATCHED", "COMPLETE"],
}

LOCATIONS = {
    "alpha": ["深圳", "广州", "香港", "洛杉矶", "芝加哥", "纽约"],
    "beta": ["伦敦", "考文垂", "曼彻斯特", "格拉斯哥"],
    "gamma": ["洛杉矶", "达拉斯", "芝加哥", "纽约"],
    "delta": ["圣保罗", "库里蒂巴", "里约热内卢"],
    "epsilon": ["東京", "大阪", "名古屋", "福岡"],
    "zeta": ["柏林", "慕尼黑", "法兰克福", "汉堡"],
    "eta": ["迪拜", "阿布扎比", "沙迦"],
    "theta": ["纽约", "纽瓦克", "波士顿", "费城"],
}

DESCRIPTIONS = {
    "alpha": ["订单已创建", "已揽收", "干线运输中", "海关查验", "派送中", "已签收"],
    "beta": ["Manifest created", "Collected from sender", "Linehaul movement",
             "Customs clearance in progress", "With courier for delivery", "Delivered"],
    "gamma": ["label created", "pickup scanned", "in transit", "customs", "out for delivery", "delivered"],
    "delta": ["Nota fiscal emitida", "Coleta realizada", "Em trânsito",
              "Aguardando liberação aduaneira", "Saiu para entrega", "Entregue"],
    "epsilon": ["送り状登録", "集荷", "幹線輸送", "通関手続", "配達出発", "配達完了"],
    "zeta": ["Shipment created", "Pickup scan", "In transit", "Customs", "Out for delivery", "POD"],
    "eta": ["Booking confirmed", "Collected", "Moving through network",
            "Customs processing", "On delivery van", "Closed/delivered"],
    "theta": ["Accepted", "Retrieved", "Routing", "In bonded transit", "Dispatched", "Complete"],
}

# 节点可见间隔（秒）；测试加速时整体乘 TIME_SCALE
BASE_VISIBILITY_GAP = 45


def _tracking_no(code: str, index: int) -> str:
    if code == "alpha":
        return f"YP{2600000000 + index:010d}"
    if code == "beta":
        return f"UK{700000000 + index:09d}"
    if code == "gamma":
        return f"GMA-{80000000 + index:08d}"
    if code == "delta":
        return f"BR{31000000000 + index:011d}BR"
    if code == "epsilon":
        return f"JP{450000000000 + index:012d}JP"
    if code == "zeta":
        return f"Z{9000000000 + index:010d}EU"
    if code == "eta":
        return f"EE{index:09X}"
    if code == "theta":
        return f"TH{600000000000 + index:012d}"
    raise ValueError(code)


def generate(per_carrier: int = 40, seed: int = 20261114,
             db_path: Path | None = None, *, in_transit_ratio: float = 0.25,
             returned_ratio: float = 0.05) -> dict:
    """生成全部真值数据并重建 simulator.db。返回统计信息。"""
    rng = random.Random(seed)
    db_path = db_path or config.SIM_DB_PATH
    Path(db_path).unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        PRAGMA foreign_keys=ON;
        CREATE TABLE sim_waybills (
            tracking_no TEXT PRIMARY KEY,
            carrier_code TEXT NOT NULL,
            final_status TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE sim_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracking_no TEXT NOT NULL,
            uid TEXT NOT NULL,
            seq INTEGER NOT NULL,
            raw_status TEXT NOT NULL,
            event_time_utc TEXT NOT NULL,
            visible_at TEXT NOT NULL,
            location TEXT NOT NULL,
            description TEXT NOT NULL,
            retracted INTEGER NOT NULL DEFAULT 0,
            UNIQUE(tracking_no, uid)
        );
        CREATE INDEX idx_sim_events ON sim_events(tracking_no, seq);
        -- 站点故障注入 / 改版状态
        CREATE TABLE sim_runtime (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    """)

    base = canonical.now_utc()
    stats: dict[str, int] = {}
    total = 0
    for code, spec in CARRIERS.items():
        chain = LIFECYCLES[code]
        count = 0
        for i in range(per_carrier):
            tracking_no = _tracking_no(code, i + 1)
            roll = rng.random()
            if roll < in_transit_ratio:
                # 在途：只生成前 2~5 个节点，之后由 advance() 继续追加
                depth = rng.randint(2, min(5, len(chain) - 1))
                raw_sequence = chain[:depth]
            elif roll < in_transit_ratio + returned_ratio:
                # 退回：走到运输中后，追加一个真实的退回末节点
                depth = rng.randint(3, 4)
                raw_sequence = list(chain[:depth])
                if code == "beta":
                    raw_sequence.append("Return to sender")
                elif code == "epsilon":
                    raw_sequence.append("差戻")
                else:
                    # 其余家无退回映射，退回单不适用，按正常签收处理
                    raw_sequence = list(chain)
            else:
                raw_sequence = list(chain)
            final_raw = raw_sequence[-1]
            final_status = canonical.map_status(code, final_raw).value
            depth = len(raw_sequence)

            # 页面时间格式全部只到分钟，真值也按分钟落库，避免秒级精度假差异
            created = (base - timedelta(days=2)).replace(second=0, microsecond=0)
            conn.execute(
                "INSERT INTO sim_waybills VALUES (?,?,?,?)",
                (tracking_no, code, final_status, canonical.to_utc_iso(created)),
            )

            gap = max(4, int(BASE_VISIBILITY_GAP * config.TIME_SCALE))
            for seq in range(depth):
                raw = raw_sequence[seq]
                event_time = (created + timedelta(hours=seq * 6 + 1)).replace(
                    second=0, microsecond=0)
                # 第一个节点立即可见，之后按 gap 逐批开放
                visible = base if seq == 0 else base + timedelta(
                    seconds=gap * (seq - 1) + rng.randint(0, max(1, gap // 3)))
                loc = LOCATIONS[code][min(seq, len(LOCATIONS[code]) - 1)]
                desc = DESCRIPTIONS[code][min(seq, len(DESCRIPTIONS[code]) - 1)]
                uid = f"{code}-{seq+1:02d}"
                conn.execute(
                    "INSERT INTO sim_events (tracking_no,uid,seq,raw_status,"
                    "event_time_utc,visible_at,location,description,retracted) "
                    "VALUES (?,?,?,?,?,?,?,?,0)",
                    (tracking_no, uid, seq + 1, raw,
                     canonical.to_utc_iso(event_time), canonical.to_utc_iso(visible),
                     loc, desc),
                )
            count += 1
        stats[code] = count
        total += count

    # eta：抽 2 单制造“节点撤回”场景（生成后由 app 的管理接口触发，
    # 这里预置一个待撤回标记，测试脚本调用 /admin/retract 生效）
    conn.execute(
        "INSERT OR REPLACE INTO sim_runtime VALUES ('retract_candidates', ?)",
        (",".join(_tracking_no("eta", i) for i in range(1, 3)),),
    )
    conn.execute("INSERT OR REPLACE INTO sim_runtime VALUES ('schema_version', 'v1')")
    conn.commit()
    conn.close()
    stats["__total__"] = total
    return stats


def advance(db_path: Path | None = None, max_new: int = 40) -> int:
    """把在途运单向前推进一个节点（模拟轨迹持续追加）。返回新增节点数。"""
    db_path = db_path or config.SIM_DB_PATH
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    now = canonical.now_utc()
    added = 0
    rows = conn.execute(
        "SELECT w.tracking_no, w.carrier_code, MAX(e.seq) AS max_seq "
        "FROM sim_waybills w JOIN sim_events e ON e.tracking_no = w.tracking_no "
        "WHERE w.final_status != 'DELIVERED' AND w.final_status != 'RETURNED' "
        "GROUP BY w.tracking_no ORDER BY RANDOM() LIMIT ?", (max_new,),
    ).fetchall()
    for row in rows:
        code = row["carrier_code"]
        chain = LIFECYCLES[code]
        next_seq = row["max_seq"] + 1
        if next_seq > len(chain):
            continue
        raw = chain[next_seq - 1]
        prev = conn.execute(
            "SELECT event_time_utc FROM sim_events WHERE tracking_no=? AND seq=?",
            (row["tracking_no"], row["max_seq"]),
        ).fetchone()
        event_time = canonical.now_utc().replace(second=0, microsecond=0)
        loc = LOCATIONS[code][min(next_seq - 1, len(LOCATIONS[code]) - 1)]
        desc = DESCRIPTIONS[code][min(next_seq - 1, len(DESCRIPTIONS[code]) - 1)]
        uid = f"{code}-{next_seq:02d}"
        cur = conn.execute(
            "INSERT OR IGNORE INTO sim_events (tracking_no,uid,seq,raw_status,"
            "event_time_utc,visible_at,location,description,retracted) "
            "VALUES (?,?,?,?,?,?,?,?,0)",
            (row["tracking_no"], uid, next_seq, raw,
             canonical.to_utc_iso(event_time), canonical.to_utc_iso(now),
             loc, desc),
        )
        added += cur.rowcount
        # 运单当前终态始终与最新节点保持一致（在途时也实时更新）
        if cur.rowcount:
            conn.execute(
                "UPDATE sim_waybills SET final_status=? WHERE tracking_no=?",
                (canonical.map_status(code, raw).value, row["tracking_no"]),
            )
    conn.commit()
    conn.close()
    return added


if __name__ == "__main__":
    print(generate())

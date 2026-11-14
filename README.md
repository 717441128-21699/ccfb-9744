# 跨境小包轨迹采集系统

面向跨境小包客服场景的运单轨迹采集平台。**只对接本仓库内置的本地仿真物流站，不访问任何真实物流网站。**

技术栈：Python 3.12+ / Playwright(Chromium) / Flask / SQLite(WAL)。

## 它解决什么

客服原本要手工到七八家物流商网站逐个复制粘贴运单轨迹。本系统：

1. 批量导入运单号，按单号规则自动识别承运商，识别不了的单独列出；
2. 各家异构页面/接口解析成**统一节点格式**，时区统一转 UTC、状态码统一映射；
3. 已签收/退回不再查，在途按状态自适应调整查询间隔；
4. 增量只追加新节点，同一节点幂等不重复入库，节点被撤回也能处理；
5. 限流自动退避重试，单家故障熔断、不影响其他家；
6. 页面改版导致解析失败会告警，**绝不默默写入空数据**；
7. 进程被杀后重启能接着跑——SQLite 租约机制保证不重复查、不漏查；
8. 跑完可与仿真站真值逐单对账，**差异为 0**。

## 目录结构

```
sim/                本地仿真物流站（8 家承运商，单 Flask 进程，端口 8765）
  generate.py       真值数据生成（运单生命周期、分批可见、撤回、推进）
  simlib.py         真值读取、本地时区渲染、令牌桶限流
  renderers.py      8 种异构页面/接口渲染
  app.py            仿真站 + 故障注入/改版/撤回/推进 管理接口
collector/          采集内核
  carriers.py       承运商注册表（单号规则、接入模式、凭据、密钥）
  canonical.py      统一状态码、时区映射、统一节点模型
  db.py             SQLite schema、连接、事务与锁重试
  importer.py       批量导入 + 单号识别
  fetcher.py        抓取层（4 种接入模式、登录、客户端限流、退避重试）
  parsers.py        各家解析器（DOM / JSON），结构不符抛 ParseError
  repository.py     节点幂等合并、撤回、状态推进、终态宽限复核
  scheduler.py      调度引擎（租约、分片 worker、熔断、告警、状态机）
  alerts.py         告警去重/冷却
  reconcile.py      逐单对账（采集库 vs 仿真真值）
  bootstrap.py      承运商配置落库
web/                Flask Web 控制台（5 个页面）
  templates/        任务调度 / 承运商配置 / 运单库 / 轨迹变更 / 告警
scripts/            启停脚本、独立演示 demo.py
tests/              单元 / 解析 / 端到端 / 杀进程恢复 / Web 测试
run_sim.py          启动仿真站
run_worker.py       启动采集 worker
run_web.py          启动 Web 控制台
cli.py              命令行工具
```

## 8 家仿真承运商的差异设计

| 代码 | 接入模式 | 特点 |
|------|----------|------|
| alpha | Playwright HTML 表格 | 状态藏在 `data-code` 属性 |
| beta | Playwright HTML（需登录） | 表单登录 + Cookie 会话 |
| gamma | requests 签名 JSON | HMAC-SHA256 签名 + 时间戳/nonce |
| delta | requests JSON | 严格 1 rps 限流，频繁 429 |
| epsilon | Playwright 日文 HTML | 日文状态码、东京时区 |
| zeta | 浏览器内 token + JSON | 页面埋 token，上下文内调接口 |
| eta | requests 快照 JSON | 整单快照，演示节点撤回 |
| theta | Playwright HTML | 会改版（v1 table → v2 div+data） |

四家 HTML 用不同 DOM 结构，四家 JSON 用不同字段名；8 个不同时区、8 套不同时间格式与状态文案。

## 快速开始

```powershell
# 0) 安装依赖 + 浏览器内核（已装可跳过）
py -3 -m pip install -r requirements.txt
py -3 -m playwright install chromium

# 1) 一键看完整链路（隔离数据、自动起站、采集、对账，约 1-2 分钟）
py -3 scripts\demo.py
```

手动分进程启动：

```powershell
# 终端 A：仿真站
py -3 scripts\start_sim.ps1
# 终端 B：采集 worker
py -3 scripts\start_worker.ps1
# 终端 C：Web 控制台 http://127.0.0.1:8800
py -3 scripts\start_web.ps1
```

命令行：

```powershell
py -3 cli.py gen-data 40        # 生成每承运商 40 单的仿真真值
py -3 cli.py import-sample     # 导入全部真值单号 + 3 个坏单号
py -3 cli.py advance           # 让仿真轨迹追加新节点
py -3 cli.py reconcile         # 逐单对账，差异为 0 才退出码 0
py -3 cli.py reset             # 清空采集库（真值保留）
```

## 测试

```powershell
py -3 -m pytest tests/ -q
```

- `test_unit.py`：单号识别、状态/时区映射、幂等合并、撤回；
- `test_parsers.py`：8 家逐家抓取解析、改版 v1 失败/v2 恢复、429、坏签名；
- `test_e2e.py`：全量采集对账为 0、追加/撤回、单家故障熔断隔离、改版告警不写空数据；
- `test_recovery.py`：子进程 worker 被 `taskkill /F` 强杀后重启，租约接管、不重不漏；
- `test_web.py`：5 个控制台页面与导入接口。

测试通过加速时间尺度（`COLLECTOR_TIME_SCALE`）在几分钟内覆盖原本数小时的调度过程。

## 关键设计

### 不重复查、不漏查（崩溃恢复）

运单被 worker 领取时原子写入 `lease_owner + lease_expires_at`。进程被杀后租约无人续约，
到期后任意存活/重启的 worker 即可重新领取。节点表对 `(waybill_id, uid)` 建唯一索引，
合并逻辑 + 数据库约束双保险，同一节点绝不重复入库。

### 增量、撤回与终态宽限

- 新 uid 插入，已存在跳过，撤回后重现则撤销撤回标记；
- eta 快照模式下，库里有、本次快照没有的节点标记 `retracted=1`（保留审计痕迹，不物理删除）；
- 进入签收/退回终态后仍有一段「宽限复核期」，用于捕获事后撤回/迟到节点，复核用尽才归档停查。

### 限流、退避与熔断

- 客户端令牌桶按承运商 rps 平滑发请求；
- 429/5xx/超时指数退避重试，404/鉴权失败这类无意义错误不重试；
- 单家连续失败到阈值即熔断一段时间，期间跳过该家，其他家 worker 完全不受影响；
- 慢站点（如 delta）支持多 worker 分片，按 `waybill.id` 取模分工。

### 改版告警而非吞空

解析器找不到预期结构即抛 `ParseError`，调度层据此写 `parse_error` 告警并保留旧轨迹，
不会用空结果覆盖。theta 在控制台把 `parser_version` 从 v1 切到 v2 即可恢复。

## 安全边界

仿真站只绑定 `127.0.0.1`，所有地址、凭据、密钥均为本仓库内置的假数据。
系统不含任何真实物流商 URL，请勿将其配置指向外部真实站点。

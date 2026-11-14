# 一键启动本地仿真物流站（8 家承运商，127.0.0.1:8765）
# 用法: py -3 scripts\start_sim.ps1
$env:PYTHONPATH = "$PSScriptRoot\.."
$env:COLLECTOR_TIME_SCALE = "1.0"
py -3 "$PSScriptRoot\..\run_sim.py"

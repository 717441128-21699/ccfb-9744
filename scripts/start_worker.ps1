# 一键启动采集 worker（调度引擎）
# 用法: py -3 scripts\start_worker.ps1
$env:PYTHONPATH = "$PSScriptRoot\.."
py -3 "$PSScriptRoot\..\run_worker.py"

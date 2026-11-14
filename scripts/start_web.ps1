# 一键启动 Web 控制台（127.0.0.1:8800）
# 用法: py -3 scripts\start_web.ps1
$env:PYTHONPATH = "$PSScriptRoot\.."
py -3 "$PSScriptRoot\..\run_web.py"

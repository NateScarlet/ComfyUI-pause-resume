@echo off
chcp 65001
cd /d "%~dp0"
echo 🚀 启动 ComfyUI 代理网关...

if not exist "python_embeded\python.exe" (
    echo 🚨 错误: 找不到 python_embeded\python.exe
    echo 请确保此脚本放置在便携版 ComfyUI 的根目录下。
    pause
    exit /b 1
)

"python_embeded\python.exe" "gateway\__main__.py"
if %errorlevel% neq 0 (
    echo 🚨 代理网关意外退出，错误码: %errorlevel%
    pause
)

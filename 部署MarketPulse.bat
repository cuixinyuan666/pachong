@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
echo 正在从 GitHub 云仓库部署 MarketPulse ...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\deploy-marketpulse.ps1" %*
if errorlevel 1 (
    echo.
    echo 部署失败，请检查网络或上方报错。
    pause
    exit /b 1
)
echo.
pause

@echo off

:: 管理者権限チェック・昇格（chcp前に実行）
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo 管理者権限で再起動します...
    powershell -NoProfile -Command "Start-Process '%~f0' -Verb RunAs"
    exit /b
)

:: ここから管理者権限で実行
chcp 65001 >nul
cd /d "%~dp0"
echo === Engage タスク一括登録 ===
echo.
echo [1/2] 通常チェック（1時間ごと）を登録中...
powershell -ExecutionPolicy Bypass -File "%~dp0タスク登録.ps1"
echo.
echo [2/2] 即時反応チェック（1分ごと）を登録中...
powershell -ExecutionPolicy Bypass -File "%~dp0即時反応タスク登録.ps1"
echo.
echo === 全タスク登録完了 ===
pause

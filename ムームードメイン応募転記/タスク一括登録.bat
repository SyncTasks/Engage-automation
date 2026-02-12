@echo off
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

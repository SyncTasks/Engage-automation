@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo === Engage応募チェック タスク登録 ===
echo.
echo 管理者権限でタスクスケジューラに登録します...
echo.
powershell -ExecutionPolicy Bypass -File "%~dp0タスク登録.ps1"
echo.
pause

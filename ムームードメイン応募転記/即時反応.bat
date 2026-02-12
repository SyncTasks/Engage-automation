@echo off
chcp 65001 >nul
cd /d "%~dp0"
call venv\Scripts\activate
python -u 応募メール処理.py --instant
exit

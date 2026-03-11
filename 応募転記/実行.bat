@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo [%date% %time%] START >> run_log.txt

if not exist venv\Scripts\activate (
    echo [%date% %time%] ERROR: venv not found >> run_log.txt
    exit /b 1
)

call venv\Scripts\activate
python -u engage_check_apply.py >> run_log.txt 2>&1
set EXITCODE=%ERRORLEVEL%

echo [%date% %time%] END (exitcode=%EXITCODE%) >> run_log.txt
exit /b %EXITCODE%

@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================
echo  エンゲージ応募転記 セットアップ＆タスク登録
echo ========================================
echo.

REM === 前提チェック ===
echo [1/6] 前提チェック中...

REM Python確認
where py >nul 2>&1
if %errorlevel%==0 (
    set PYTHON_CMD=py
) else (
    where python >nul 2>&1
    if %errorlevel%==0 (
        set PYTHON_CMD=python
    ) else (
        echo [エラー] Pythonが見つかりません
        echo https://www.python.org/downloads/ からインストールしてください
        pause
        exit /b 1
    )
)
echo   Python: OK

REM Credentials.json確認
if not exist Credentials.json (
    echo [エラー] Credentials.json が見つかりません
    echo Google Cloud サービスアカウントのJSONキーを Credentials.json として配置してください
    pause
    exit /b 1
)
echo   Credentials.json: OK

REM .env確認
if not exist .env (
    echo [エラー] .env ファイルが見つかりません
    echo .env ファイルに TALENT_DB_SPREADSHEET_ID を設定してください
    pause
    exit /b 1
)
echo   .env: OK

REM Chrome確認
if exist "C:\Program Files\Google\Chrome\Application\chrome.exe" (
    echo   Google Chrome: OK
) else (
    echo   [警告] Google Chromeが見つかりません（reCAPTCHA回避に必要）
    echo   https://www.google.com/chrome/ からインストールしてください
    echo.
    set /p CONTINUE="Chromeなしで続行しますか？ (Y/N): "
    if /i not "%CONTINUE%"=="Y" (
        pause
        exit /b 1
    )
)
echo.

REM === Python環境セットアップ ===
echo [2/6] Python仮想環境を作成中...
%PYTHON_CMD% -m venv venv
call venv\Scripts\activate
echo.

echo [3/6] 依存パッケージをインストール中...
pip install -r 必要パッケージ.txt
echo.

echo [4/6] Playwrightブラウザをインストール中...
playwright install chromium
echo.

REM === 動作確認 ===
echo [5/6] 動作確認（インポートチェック）...
python -c "from engage_check_apply import main; print('OK: スクリプト読み込み成功')"
if %errorlevel% neq 0 (
    echo [エラー] スクリプトの読み込みに失敗しました
    pause
    exit /b 1
)
echo.

REM === タスクスケジューラ登録 ===
echo [6/6] タスクスケジューラに登録中...
echo   （Administratorのパスワード入力が必要です）
echo.
powershell -ExecutionPolicy Bypass -File "%~dp0タスク登録.ps1"
if %errorlevel% neq 0 (
    echo [エラー] タスク登録に失敗しました
    pause
    exit /b 1
)

echo.
echo ========================================
echo  セットアップ完了！
echo ========================================
echo.
echo   1時間ごとに自動実行されます
echo   手動実行: "実行.bat" をダブルクリック
echo   ログ: run_log.txt / engage_scraper_YYYY-MM-DD.log
echo.
pause

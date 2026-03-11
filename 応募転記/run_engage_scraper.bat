@echo off
chcp 65001 >nul
setlocal

REM バッチファイルのあるディレクトリに移動
cd /d "%~dp0"

echo ================================
echo Engage Scraper を起動します...
echo ================================
echo.

REM Pythonの存在確認
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Pythonが見つかりません。
    echo Pythonをインストールしてください。
    pause
    exit /b 1
)

REM 仮想環境があれば有効化（venv または .venv）
if exist "venv\Scripts\activate.bat" (
    echo 仮想環境を有効化中...
    call venv\Scripts\activate.bat
) else if exist ".venv\Scripts\activate.bat" (
    echo 仮想環境を有効化中...
    call .venv\Scripts\activate.bat
) else if exist "..\venv\Scripts\activate.bat" (
    echo 仮想環境を有効化中...
    call ..\venv\Scripts\activate.bat
) else if exist "..\.venv\Scripts\activate.bat" (
    echo 仮想環境を有効化中...
    call ..\.venv\Scripts\activate.bat
)

REM スクリプトを実行
echo.
echo スクレイピングを開始します...
echo.
python engage_check_apply.py

echo.
echo ================================
echo 処理が完了しました
echo ================================
pause

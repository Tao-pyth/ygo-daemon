@echo off
setlocal

REM ==============================================
REM ygo-daemon: dict-build (scripts 配置用)
REM ==============================================

REM 1) リポジトリ直下へ移動（scripts の1階層上）
pushd "%~dp0.."

REM 2) venv が存在すれば有効化
if exist ".venv\Scripts\activate.bat" (
    call ".venv\Scripts\activate.bat" >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] venv activate failed.
        popd
        exit /b 1
    )
)

REM 3) UTF-8強制（日本語ログ対策）
set PYTHONUTF8=1

REM 4) dict-build 実行
python main.py dict-build
set RC=%ERRORLEVEL%

popd
exit /b %RC%

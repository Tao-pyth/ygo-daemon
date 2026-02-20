@echo off
setlocal

REM =========================================================
REM ygo-daemon: dict-dump with timestamp (scripts 配置用)
REM 出力: data\exports\dict_dump_YYYYMMDD_HHMMSS.jsonl
REM =========================================================

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

REM 3) 文字化け対策（任意）
set PYTHONUTF8=1

REM 4) タイムスタンプ生成（ロケール非依存で安全）
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyyMMdd_HHmmss')"') do set TS=%%i

REM 5) 出力先（dict-dump は --out のパスへそのまま出力する）
set OUTFILE=data\exports\dict_dump_%TS%.jsonl

REM 6) ダンプ実行
REM 例：管理テーブルをまとめて出力（必要に応じて --tables を変更）
python main.py dict-dump --tables dsl_dictionary_patterns,dsl_dictionary_terms,kv_store --out "%OUTFILE%" --format jsonl
set RC=%ERRORLEVEL%

popd
exit /b %RC%

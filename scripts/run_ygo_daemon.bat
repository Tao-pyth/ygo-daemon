@echo off
cd /d %~dp0\..

call .venv\Scripts\activate

if not exist data\logs mkdir data\logs

python main.py run >> data\logs\daemon.log 2>&1

deactivate

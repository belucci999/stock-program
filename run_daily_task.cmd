@echo off
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
if not exist logs mkdir logs
python run_scheduled_analysis.py >> logs\scheduler_%date:~0,4%%date:~5,2%%date:~8,2%.log 2>&1

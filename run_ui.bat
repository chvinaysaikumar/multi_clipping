@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" clipwatcher.py --ui
) else (
    python clipwatcher.py --ui
)
pause

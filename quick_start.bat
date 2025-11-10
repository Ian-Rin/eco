@echo off
setlocal enabledelayedexpansion

rem Resolve repository root (directory containing this script)
set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

set "PYTHON_CMD="
if defined PYTHON_BIN (
    set "PYTHON_CMD=%PYTHON_BIN%"
) else (
    for %%P in (python py) do (
        %%P --version >nul 2>&1
        if not errorlevel 1 (
            set "PYTHON_CMD=%%P"
            goto python_detected
        )
    )
    echo [ERROR] Could not find a Python interpreter. Set PYTHON_BIN to specify one.
    exit /b 1
)

:python_detected
"%PYTHON_CMD%" --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python interpreter '%PYTHON_CMD%' not found or failed to run.
    exit /b 1
)

call :run_python fetch_incremental.py || exit /b 1
call :run_python ak_repurchase_plans_incremental.py || exit /b 1

echo [INFO] Starting FastAPI server via uvicorn
uvicorn app_fastapi:app --host 0.0.0.0 --port 8000
exit /b %errorlevel%

:run_python
set "SCRIPT=%~1"
echo [INFO] Running %SCRIPT%
"%PYTHON_CMD%" "%SCRIPT%"
exit /b %errorlevel%

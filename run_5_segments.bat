@echo off
setlocal

REM 自动运行 run1~run5，并将 log/*.csv 归档到 results/raw/<strategy>/runN/
REM 用法:
REM   run_5_segments.bat
REM   run_5_segments.bat SOTA-Lock
REM   run_5_segments.bat Fine-Tune-Lock selectedTxs_300K.csv
REM   run_5_segments.bat Fine-Tune-Lock selectedTxs_300K.csv 5 20000 results\raw

set "STRATEGY=%~1"
if "%STRATEGY%"=="" set "STRATEGY=SOTA-Lock"

set "DATASET=%~2"
if "%DATASET%"=="" set "DATASET=selectedTxs_300K.csv"

set "RUNS=%~3"
if "%RUNS%"=="" set "RUNS=5"

set "WINDOW=%~4"
if "%WINDOW%"=="" set "WINDOW=20000"

set "RAW_ROOT=%~5"
if "%RAW_ROOT%"=="" set "RAW_ROOT=results\raw"

echo.
echo ============================================
echo Auto run segments
echo Strategy : %STRATEGY%
echo Dataset  : %DATASET%
echo Runs     : %RUNS%
echo Window   : %WINDOW%
echo RawRoot  : %RAW_ROOT%
echo ============================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\run_5_segments.ps1" -RepoRoot "." -Dataset "%DATASET%" -Strategy "%STRATEGY%" -Runs %RUNS% -Window %WINDOW% -RawRoot "%RAW_ROOT%"
if errorlevel 1 (
    echo.
    echo [ERROR] auto run failed.
    exit /b 1
)

echo.
echo [DONE] auto runs finished.
echo.
exit /b 0


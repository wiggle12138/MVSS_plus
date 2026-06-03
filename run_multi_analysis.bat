@echo off
setlocal EnableExtensions

REM Run multi-run summary and plotting.
REM Usage:
REM   run_multi_analysis.bat
REM   run_multi_analysis.bat results\raw results\multi_run_summary

set "ROOT_DIR=%~1"
if "%ROOT_DIR%"=="" set "ROOT_DIR=results\raw"

set "OUT_DIR=%~2"
if "%OUT_DIR%"=="" set "OUT_DIR=results\multi_run_summary"

echo.
echo ============================================
echo [1/2] Summarize multi-run metrics
echo root_dir = %ROOT_DIR%
echo out_dir  = %OUT_DIR%
echo ============================================
echo.

python "scripts\summarize_multi_runs.py" --root-dir "%ROOT_DIR%" --out-dir "%OUT_DIR%"
if errorlevel 1 goto :fail

echo.
echo ============================================
echo [2/2] Generate comparison figures
echo ============================================
echo.

python "scripts\plot_multi_runs.py" --summary-csv "%OUT_DIR%\strategy_summary.csv" --run-csv "%OUT_DIR%\run_metrics.csv" --out-dir "%OUT_DIR%\figures"
if errorlevel 1 goto :fail

echo.
echo [DONE] Generated tables and figures:
echo - %OUT_DIR%\run_metrics.csv
echo - %OUT_DIR%\strategy_summary.csv
echo - %OUT_DIR%\strategy_summary.md
echo - %OUT_DIR%\figures\*.png
echo - %OUT_DIR%\figures\figures_manifest.md
echo.
exit /b 0

:fail
echo.
echo [ERROR] Failed. Check folder structure and Python env.
echo Expected structure:
echo   results\raw\SOTA-Lock\run1\*.csv
echo   results\raw\Fine-tuned-Lock\run1\*.csv
echo.
exit /b 1


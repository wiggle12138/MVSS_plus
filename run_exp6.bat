@echo off
setlocal EnableExtensions

REM Exp6: MVSS-Delta DeltaAggregateWindowMs sensitivity (probe ON).
REM Usage:
REM   run_exp6.bat
REM   run_exp6.bat selectedTxs_300K.csv MVSS-Delta "2" 2 "0,50,100,200,500" 3
REM   set OUT_ROOT=results/exp6_scale_4x4_full/raw
REM   run_exp6.bat selectedTxs_300K.csv MVSS-Delta "4" 4 "0,50,100,200,500" 3
REM Args: %1 dataset  %2 strategy  %3 shards  %4 nodes/shard  %5 windows ms  %6 runs

set "DATASET=%~1"
if "%DATASET%"=="" set "DATASET=selectedTxs_300K.csv"

set "STRATEGY=%~2"
if "%STRATEGY%"=="" set "STRATEGY=MVSS-Delta"

set "SHARDS=%~3"
if "%SHARDS%"=="" set "SHARDS=2"

set "NODES_PER_SHARD=%~4"
if "%NODES_PER_SHARD%"=="" set "NODES_PER_SHARD=2"

set "WINDOWS=%~5"
if "%WINDOWS%"=="" set "WINDOWS=0,50,100,200,500"

set "RUNS=%~6"
if "%RUNS%"=="" set "RUNS=3"

if %RUNS% GTR 5 (
  if /I not "%ALLOW_LONG_RUN%"=="1" (
    echo [SAFEGUARD] RUNS=%RUNS% may take very long. Set ALLOW_LONG_RUN=1 to proceed.
    exit /b 1
  )
)

if "%MAX_INJECT_TXS%"=="" set "MAX_INJECT_TXS=24000"
if "%SYNC_PROBE_MAX_ACCOUNTS%"=="" set "SYNC_PROBE_MAX_ACCOUNTS=50"
if "%NODE_WAIT_SEC%"=="" set "NODE_WAIT_SEC=12"
if "%OUT_ROOT%"=="" set "OUT_ROOT=results/exp6_sensitivity/raw"
if "%METRICS_OUT_DIR%"=="" set "METRICS_OUT_DIR=results/exp6_sensitivity/metrics"

echo.
echo ============================================
echo Exp6 window sensitivity
echo dataset              = %DATASET%
echo strategy             = %STRATEGY%
echo shard list           = %SHARDS%
echo nodes per shard      = %NODES_PER_SHARD%
echo windows(ms)          = %WINDOWS%
echo runs per setting     = %RUNS%
echo max inject txs       = %MAX_INJECT_TXS%
echo sync probe accounts  = %SYNC_PROBE_MAX_ACCOUNTS%
echo out root             = %OUT_ROOT%
echo ============================================
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\run_probe_matrix.ps1" ^
  -RepoRoot "." ^
  -Dataset "%DATASET%" ^
  -Strategy "%STRATEGY%" ^
  -ShardNums "%SHARDS%" ^
  -NodesPerShard %NODES_PER_SHARD% ^
  -Windows "%WINDOWS%" ^
  -Runs %RUNS% ^
  -NodeWaitSec %NODE_WAIT_SEC% ^
  -MaxInjectTxs %MAX_INJECT_TXS% ^
  -SyncProbeMaxAccounts %SYNC_PROBE_MAX_ACCOUNTS% ^
  -OutRoot "%OUT_ROOT%" ^
  -MetricsOutDir "%METRICS_OUT_DIR%"
if errorlevel 1 (
  echo [ERROR] exp6 run failed.
  exit /b 1
)

echo [DONE] exp6 finished. Raw: %OUT_ROOT%
exit /b 0

@echo off
setlocal EnableExtensions

REM Exp2: MVSS vs MVSS-Delta under high migration probe load.
REM Usage:
REM   run_exp2.bat
REM   run_exp2.bat selectedTxs_300K.csv "50" 3 200
REM Args: %1 dataset  %2 probe accounts  %3 runs  %4 Delta window ms
REM Env: SHARDS=4  NODES_PER_SHARD=4  MAX_INJECT_TXS=24000  NODE_WAIT_SEC=12

set "DATASET=%~1"
if "%DATASET%"=="" set "DATASET=selectedTxs_300K.csv"

set "PROBES=%~2"
if "%PROBES%"=="" set "PROBES=50"

set "RUNS=%~3"
if "%RUNS%"=="" set "RUNS=3"

set "DELTA_WINDOW=%~4"
if "%DELTA_WINDOW%"=="" set "DELTA_WINDOW=200"

if "%SHARDS%"=="" set "SHARDS=4"
if "%NODES_PER_SHARD%"=="" set "NODES_PER_SHARD=4"
if "%MAX_INJECT_TXS%"=="" set "MAX_INJECT_TXS=24000"
if "%NODE_WAIT_SEC%"=="" set "NODE_WAIT_SEC=12"

echo.
echo ============================================
echo Exp2: MVSS vs MVSS-Delta
echo dataset         = %DATASET%
echo shards          = %SHARDS%
echo nodes per shard = %NODES_PER_SHARD%
echo probe accounts  = %PROBES%
echo runs            = %RUNS%
echo Delta window    = %DELTA_WINDOW% ms
echo max inject txs  = %MAX_INJECT_TXS%
echo ============================================
echo.

set "PROBES_SP=%PROBES:,= %"
for %%P in (%PROBES_SP%) do (
  set "SYNC_PROBE_MAX_ACCOUNTS=%%P"
  set "OUT_ROOT=results/exp2_concurrency/raw/probe_%%P/strategy_MVSS"
  set "METRICS_OUT_DIR=results/exp2_concurrency/metrics"
  powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\run_probe_matrix.ps1" ^
    -RepoRoot "." ^
    -Dataset "%DATASET%" ^
    -Strategy "MVSS" ^
    -ShardNums "%SHARDS%" ^
    -NodesPerShard %NODES_PER_SHARD% ^
    -Windows "0" ^
    -Runs %RUNS% ^
    -NodeWaitSec %NODE_WAIT_SEC% ^
    -MaxInjectTxs %MAX_INJECT_TXS% ^
    -SyncProbeMaxAccounts %%P ^
    -OutRoot "results/exp2_concurrency/raw/probe_%%P/strategy_MVSS" ^
    -MetricsOutDir "results/exp2_concurrency/metrics"
  if errorlevel 1 exit /b 1

  powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\run_probe_matrix.ps1" ^
    -RepoRoot "." ^
    -Dataset "%DATASET%" ^
    -Strategy "MVSS-Delta" ^
    -ShardNums "%SHARDS%" ^
    -NodesPerShard %NODES_PER_SHARD% ^
    -Windows "%DELTA_WINDOW%" ^
    -Runs %RUNS% ^
    -NodeWaitSec %NODE_WAIT_SEC% ^
    -MaxInjectTxs %MAX_INJECT_TXS% ^
    -SyncProbeMaxAccounts %%P ^
    -OutRoot "results/exp2_concurrency/raw/probe_%%P/strategy_MVSS-Delta" ^
    -MetricsOutDir "results/exp2_concurrency/metrics"
  if errorlevel 1 exit /b 1
)

echo [DONE] exp2 finished. Raw: results\exp2_concurrency\raw\
exit /b 0

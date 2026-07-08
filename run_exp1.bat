@echo off
setlocal EnableExtensions

REM Exp1 scaling: probe OFF, outputs under results/exp1_scaling/
REM Usage:
REM   run_exp1.bat
REM   run_exp1.bat selectedTxs_300K.csv "SOTA-Lock,MVSS" 4 4 1 20000
REM   run_exp1.bat selectedTxs_300K.csv "" 4 4 1 24000 dryrun
REM Args:
REM   %1 dataset
REM   %2 strategies csv (default all four)
REM   %3 shard num
REM   %4 nodes per shard
REM   %5 runs per strategy
REM   %6 max inject txs
REM   %7 dryrun OR single inject speed when numeric (e.g. 800)
REM   %8 run start (optional)
REM   %9 shard list csv (optional, overrides %3)
REM Env: INJECT_SPEED_LIST=200,400,800  SHARD_LIST=4,6,8  ALLOW_LONG_RUN=1

set "DATASET=%~1"
if "%DATASET%"=="" set "DATASET=selectedTxs_300K.csv"

set "STRATEGIES=%~2"
if "%STRATEGIES%"=="" set "STRATEGIES=SOTA-Lock,Fine-tuned-Lock,MVSS,MVSS-Delta"

set "SHARD_NUM=%~3"
if "%SHARD_NUM%"=="" set "SHARD_NUM=4"

set "NODES_PER_SHARD=%~4"
if "%NODES_PER_SHARD%"=="" set "NODES_PER_SHARD=4"

set "RUNS=%~5"
if "%RUNS%"=="" set "RUNS=3"

set "MAX_INJECT_TXS=%~6"
if "%MAX_INJECT_TXS%"=="" set "MAX_INJECT_TXS=24000"

set "RUN_START=%RUN_START%"
if "%RUN_START%"=="" set "RUN_START=1"
if not "%~8"=="" set "RUN_START=%~8"

set "INJECT_SPEED=%INJECT_SPEED%"
if "%INJECT_SPEED%"=="" set "INJECT_SPEED=400"

set "SHARD_LIST=%SHARD_LIST%"
if "%SHARD_LIST%"=="" set "SHARD_LIST=%SHARD_NUM%"
if not "%~9"=="" set "SHARD_LIST=%~9"

set "INJECT_SPEED_LIST=%INJECT_SPEED_LIST%"
if "%INJECT_SPEED_LIST%"=="" set "INJECT_SPEED_LIST=%INJECT_SPEED%"
if /I "%~7"=="dryrun" (
  set "PS_DRY=-DryRun"
) else if not "%~7"=="" (
  echo %~7| findstr /r "^[0-9][0-9]*$" >nul && set "INJECT_SPEED_LIST=%~7"
)

set "NODE_WAIT_SEC=%NODE_WAIT_SEC%"
if "%NODE_WAIT_SEC%"=="" set "NODE_WAIT_SEC=12"

set "DELTA_WINDOW_MS=%DELTA_WINDOW_MS%"
if "%DELTA_WINDOW_MS%"=="" set "DELTA_WINDOW_MS=200"

set "EXP_ROOT=%EXP_ROOT%"
if "%EXP_ROOT%"=="" set "EXP_ROOT=results\exp1_scaling"

set "RUN_TIMEOUT_SEC=%RUN_TIMEOUT_SEC%"
if "%RUN_TIMEOUT_SEC%"=="" set "RUN_TIMEOUT_SEC=900"

for /f %%a in ('powershell -NoProfile -Command "('%STRATEGIES%' -split ',').Count"') do set "STRAT_COUNT=%%a"
for /f %%a in ('powershell -NoProfile -Command "('%SHARD_LIST%' -split ',').Count"') do set "SHARD_COUNT=%%a"
for /f %%a in ('powershell -NoProfile -Command "('%INJECT_SPEED_LIST%' -split ',').Count"') do set "SPEED_COUNT=%%a"
set /a TOTAL_RUNS=%STRAT_COUNT%*%RUNS%*%SHARD_COUNT%*%SPEED_COUNT%

if %TOTAL_RUNS% GTR 5 (
  if /I not "%ALLOW_LONG_RUN%"=="1" (
    echo [SAFEGUARD] Total runs=%TOTAL_RUNS% ^(%STRAT_COUNT% strategies x %RUNS% runs^).
    echo Set ALLOW_LONG_RUN=1 to proceed with full batch.
    echo Smoke example:
    echo   run_exp1.bat %DATASET% SOTA-Lock 4 4 1 %MAX_INJECT_TXS%
    exit /b 1
  )
)

echo.
echo ============================================
echo Exp1 scaling
echo dataset           = %DATASET%
echo strategies        = %STRATEGIES%
echo shard list        = %SHARD_LIST%
echo nodes per shard   = %NODES_PER_SHARD%
echo speed list        = %INJECT_SPEED_LIST%
echo runs/strategy     = %RUNS%
echo max inject txs    = %MAX_INJECT_TXS%
echo exp root          = %EXP_ROOT%
echo total runs        = %TOTAL_RUNS%
echo ============================================
echo.

set "PS_DRY="
if /I "%DRY_RUN%"=="1" set "PS_DRY=-DryRun"
if /I "%~7"=="dryrun" set "PS_DRY=-DryRun"

powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\run_exp1_scaling.ps1" ^
  -RepoRoot "." ^
  -Dataset "%DATASET%" ^
  -Strategies "%STRATEGIES%" ^
  -ShardNum %SHARD_NUM% ^
  -ShardNums "%SHARD_LIST%" ^
  -NodesPerShard %NODES_PER_SHARD% ^
  -Runs %RUNS% ^
  -RunStart %RUN_START% ^
  -NodeWaitSec %NODE_WAIT_SEC% ^
  -MaxInjectTxs %MAX_INJECT_TXS% ^
  -InjectSpeed %INJECT_SPEED% ^
  -InjectSpeeds "%INJECT_SPEED_LIST%" ^
  -DeltaWindowMs %DELTA_WINDOW_MS% ^
  -RunTimeoutSec %RUN_TIMEOUT_SEC% ^
  -ExpRoot "%EXP_ROOT%" ^
  %PS_DRY%
if errorlevel 1 (
  echo [ERROR] exp1 run failed.
  exit /b 1
)

echo [DONE] exp1 finished. Output: %EXP_ROOT%
exit /b 0

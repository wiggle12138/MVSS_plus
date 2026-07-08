@echo off
setlocal EnableExtensions

REM Exp4: real Ethereum BlockTransaction replay (probe OFF).
REM Usage:
REM   run_exp4.bat
REM   run_exp4.bat dryrun
REM Env: DATASET=13000000to13249999_BlockTransaction.csv  ALLOW_LONG_RUN=1

set "ACTION=%~1"

set "DATASET=%DATASET%"
if "%DATASET%"=="" set "DATASET=data\exp4\13000000to13249999_BlockTransaction_head150k.csv"

set "DATASET_TOKEN=%DATASET_TOKEN%"
if "%DATASET_TOKEN%"=="" set "DATASET_TOKEN=eth_head150k"

set "STRATEGIES=%STRATEGIES%"
if "%STRATEGIES%"=="" set "STRATEGIES=SOTA-Lock,Fine-tuned-Lock,MVSS,MVSS-Delta"
set "SHARDS=%SHARDS%"
if "%SHARDS%"=="" set "SHARDS=8"
set "NODES_PER_SHARD=%NODES_PER_SHARD%"
if "%NODES_PER_SHARD%"=="" set "NODES_PER_SHARD=4"
set "RUNS=%RUNS%"
if "%RUNS%"=="" set "RUNS=3"
set "MAX_INJECT_TXS=%MAX_INJECT_TXS%"
if "%MAX_INJECT_TXS%"=="" set "MAX_INJECT_TXS=50000"
set "INJECT_SPEED=%INJECT_SPEED%"
if "%INJECT_SPEED%"=="" set "INJECT_SPEED=800"
set "NODE_WAIT_SEC=%NODE_WAIT_SEC%"
if "%NODE_WAIT_SEC%"=="" set "NODE_WAIT_SEC=60"
set "RUN_TIMEOUT_SEC=%RUN_TIMEOUT_SEC%"
if "%RUN_TIMEOUT_SEC%"=="" set "RUN_TIMEOUT_SEC=1200"

if not exist "%DATASET%" (
  echo [ERROR] Dataset not found: %DATASET%
  exit /b 1
)

for /f %%a in ('powershell -NoProfile -Command "('%STRATEGIES%' -split ',').Count"') do set "STRAT_COUNT=%%a"
set /a TOTAL_RUNS=%STRAT_COUNT%*%RUNS%
if %TOTAL_RUNS% GTR 5 (
  if /I not "%ALLOW_LONG_RUN%"=="1" (
    if /I not "%ACTION%"=="dryrun" (
      echo [SAFEGUARD] Total runs=%TOTAL_RUNS%. Set ALLOW_LONG_RUN=1 to proceed.
      exit /b 1
    )
  )
)

set "PS_DRY="
if /I "%ACTION%"=="dryrun" set "PS_DRY=-DryRun"
if /I "%DRY_RUN%"=="1" set "PS_DRY=-DryRun"

powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\run_exp4_eth.ps1" ^
  -RepoRoot "." -Dataset "%DATASET%" -DatasetToken "%DATASET_TOKEN%" ^
  -Strategies "%STRATEGIES%" -ShardNum %SHARDS% -NodesPerShard %NODES_PER_SHARD% ^
  -Runs %RUNS% -MaxInjectTxs %MAX_INJECT_TXS% -InjectSpeed %INJECT_SPEED% ^
  -NodeWaitSec %NODE_WAIT_SEC% -RunTimeoutSec %RUN_TIMEOUT_SEC% %PS_DRY%
exit /b %errorlevel%

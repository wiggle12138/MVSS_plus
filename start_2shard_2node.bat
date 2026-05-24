@echo off
setlocal

REM 2 shards x 2 nodes + client.
REM Order: start 4 shard nodes first, wait, then client (fixes Sendtime dial refused on 8010).
REM Current code gates the FIRST block on first tx arrival (ClientSendTX=true), so startup empty-block storms are reduced.
REM N0 may still fail TcpDial to 8800 before client listens (logged only).
REM Usage: start_2shard_2node.bat
REM         start_2shard_2node.bat 20W.csv
REM Slow cold build: set NODE_WAIT_SEC=60 before running this bat.

pushd "%~dp0"

set "SHARD_NUM=2"
set "MALICIOUS_NUM=0"
set "TEST_FILE=%~1"
if "%TEST_FILE%"=="" set "TEST_FILE=selectedTxs_300K.csv"

if "%NODE_WAIT_SEC%"=="" set "NODE_WAIT_SEC=4"

if not exist "%TEST_FILE%" (
    echo [ERROR] Dataset file not found: "%TEST_FILE%"
    pause
    popd
    exit /b 1
)

echo.
echo ============================================
echo Nodes first, then CLIENT (wait %NODE_WAIT_SEC%s for go run + listen only)
echo Dataset: %TEST_FILE%
echo ============================================
echo.

start "MVSS S0-N0" cmd /k "cd /d ""%CD%"" && go run main.go -S %SHARD_NUM% -s S0 -f %MALICIOUS_NUM% -n N0 -t ""%TEST_FILE%"""
start "MVSS S0-N1" cmd /k "cd /d ""%CD%"" && go run main.go -S %SHARD_NUM% -s S0 -f %MALICIOUS_NUM% -n N1 -t ""%TEST_FILE%"""
start "MVSS S1-N0" cmd /k "cd /d ""%CD%"" && go run main.go -S %SHARD_NUM% -s S1 -f %MALICIOUS_NUM% -n N0 -t ""%TEST_FILE%"""
start "MVSS S1-N1" cmd /k "cd /d ""%CD%"" && go run main.go -S %SHARD_NUM% -s S1 -f %MALICIOUS_NUM% -n N1 -t ""%TEST_FILE%"""

echo Started 4 node windows. Waiting %NODE_WAIT_SEC%s for 8010/8011/8020/8021 to listen...
timeout /t %NODE_WAIT_SEC% /nobreak >nul

start "MVSS CLIENT" cmd /k "cd /d ""%CD%"" && go run main.go -S %SHARD_NUM% -f %MALICIOUS_NUM% -c -t ""%TEST_FILE%"""

echo Started CLIENT (127.0.0.1:8800). Stop with Ctrl+C in each window.
echo.

popd
endlocal

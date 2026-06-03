param(
    [string]$RepoRoot = ".",
    [string]$Dataset = "selectedTxs_300K.csv",
    [string]$Strategy = "SOTA-Lock",
    [int]$Runs = 5,
    [int]$Window = 20000,
    [string]$RawRoot = "results/raw",
    [int]$NodeWaitSec = 6
)

$ErrorActionPreference = "Stop"

function Resolve-PathSafe([string]$Base, [string]$Relative) {
    return [System.IO.Path]::GetFullPath((Join-Path $Base $Relative))
}

function Cleanup-RunState([string]$RootDir) {
    $logDir = Join-Path $RootDir "log"
    if (Test-Path $logDir) {
        Get-ChildItem $logDir -File -Filter "*.csv" -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue
    }

    Get-ChildItem $RootDir -Directory -Filter "*_blockchain_db" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

    $trieDir = Join-Path $RootDir "record/triedb"
    if (Test-Path $trieDir) {
        Get-ChildItem $trieDir -Recurse -Force -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Wait-And-CleanupProcesses([System.Diagnostics.Process[]]$NodeProcs, [System.Diagnostics.Process]$ClientProc) {
    if ($null -ne $ClientProc) {
        Wait-Process -Id $ClientProc.Id
    }

    foreach ($p in $NodeProcs) {
        if ($null -eq $p) { continue }
        try {
            Wait-Process -Id $p.Id -Timeout 20 -ErrorAction Stop
        } catch {
            try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
        }
    }
}

$repo = [System.IO.Path]::GetFullPath($RepoRoot)
$datasetPath = Resolve-PathSafe $repo $Dataset
if (-not (Test-Path $datasetPath)) {
    throw "Dataset file not found: $datasetPath"
}

$rawRootPath = Resolve-PathSafe $repo $RawRoot
New-Item -ItemType Directory -Path $rawRootPath -Force | Out-Null
$strategyRoot = Join-Path $rawRootPath $Strategy
New-Item -ItemType Directory -Path $strategyRoot -Force | Out-Null

Write-Host ""
Write-Host "============================================"
Write-Host "Auto run segments"
Write-Host "RepoRoot : $repo"
Write-Host "Dataset  : $datasetPath"
Write-Host "Strategy : $Strategy"
Write-Host "Runs     : $Runs"
Write-Host "Window   : $Window"
Write-Host "OutRoot  : $strategyRoot"
Write-Host "============================================"
Write-Host ""

for ($run = 1; $run -le $Runs; $run++) {
    $offset = ($run - 1) * $Window
    $runDir = Join-Path $strategyRoot ("run" + $run)
    New-Item -ItemType Directory -Path $runDir -Force | Out-Null

    Write-Host ""
    Write-Host "---- Run $run / $Runs  (offset=$offset, window=$Window) ----"

    Cleanup-RunState $repo

    $nodeArgs = @(
        @("run", "main.go", "-S", "2", "-s", "S0", "-f", "0", "-n", "N0", "-t", $datasetPath, "-m", $Strategy),
        @("run", "main.go", "-S", "2", "-s", "S0", "-f", "0", "-n", "N1", "-t", $datasetPath, "-m", $Strategy),
        @("run", "main.go", "-S", "2", "-s", "S1", "-f", "0", "-n", "N0", "-t", $datasetPath, "-m", $Strategy),
        @("run", "main.go", "-S", "2", "-s", "S1", "-f", "0", "-n", "N1", "-t", $datasetPath, "-m", $Strategy)
    )

    $nodeProcs = @()
    for ($i = 0; $i -lt $nodeArgs.Count; $i++) {
        $nodeStdOut = Join-Path $runDir ("node" + $i + ".out.log")
        $nodeStdErr = Join-Path $runDir ("node" + $i + ".err.log")
        $proc = Start-Process -FilePath "go" -ArgumentList $nodeArgs[$i] -WorkingDirectory $repo -PassThru -NoNewWindow -RedirectStandardOutput $nodeStdOut -RedirectStandardError $nodeStdErr
        $nodeProcs += $proc
    }

    Start-Sleep -Seconds $NodeWaitSec

    $clientStdOut = Join-Path $runDir "client.out.log"
    $clientStdErr = Join-Path $runDir "client.err.log"
    $clientArgs = @(
        "run", "main.go",
        "-S", "2",
        "-f", "0",
        "-c",
        "-t", $datasetPath,
        "-m", $Strategy,
        "--maxInjectTxs", $Window,
        "--injectStartTx", $offset
    )
    $clientProc = Start-Process -FilePath "go" -ArgumentList $clientArgs -WorkingDirectory $repo -PassThru -NoNewWindow -RedirectStandardOutput $clientStdOut -RedirectStandardError $clientStdErr

    Wait-And-CleanupProcesses -NodeProcs $nodeProcs -ClientProc $clientProc

    $logDir = Join-Path $repo "log"
    if (-not (Test-Path $logDir)) {
        throw "log directory not found after run: $logDir"
    }
    Get-ChildItem $logDir -File -Filter "*.csv" | ForEach-Object {
        Copy-Item $_.FullName (Join-Path $runDir $_.Name) -Force
    }

    $csvCount = (Get-ChildItem $runDir -File -Filter "*.csv" | Measure-Object).Count
    Write-Host "Run $run finished. Copied CSV files: $csvCount"
}

Write-Host ""
Write-Host "[DONE] All runs completed."
Write-Host "Output directory: $strategyRoot"
Write-Host ""


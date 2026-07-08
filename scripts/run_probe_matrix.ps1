param(
    [string]$RepoRoot = ".",
    [string]$Dataset = "selectedTxs_300K.csv",
    [string]$Strategy = "MVSS-Delta",
    [string]$ShardNums = "2,4",
    [int]$NodesPerShard = 2,
    [string]$Windows = "0,50,100,200,500",
    [int]$Runs = 1,
    [int]$NodeWaitSec = 8,
    [int]$MaxInjectTxs = 20000,
    [int]$SyncProbeMaxAccounts = 3,
    [int]$RunTimeoutSec = 900,
    [string]$OutRoot = "results/exp6_sensitivity/raw",
    [string]$MetricsOutDir = "results/exp6_sensitivity/metrics"
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

function Stop-RunProcesses([System.Diagnostics.Process[]]$NodeProcs, [System.Diagnostics.Process]$ClientProc) {
    if ($null -ne $ClientProc) {
        try { Stop-Process -Id $ClientProc.Id -Force -ErrorAction SilentlyContinue } catch {}
    }
    foreach ($p in $NodeProcs) {
        if ($null -eq $p) { continue }
        try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
    }
}

function Wait-And-CleanupProcesses([System.Diagnostics.Process[]]$NodeProcs, [System.Diagnostics.Process]$ClientProc, [int]$RunTimeoutSec) {
    $deadline = (Get-Date).AddSeconds($RunTimeoutSec)
    while ($true) {
        if ($null -ne $ClientProc) {
            try { $ClientProc.Refresh() } catch {}
            if ($ClientProc.HasExited) {
                $clientExitCode = 0
                try { $clientExitCode = [int]$ClientProc.ExitCode } catch { $clientExitCode = 0 }
                if ($null -eq $clientExitCode) { $clientExitCode = 0 }
                if ($clientExitCode -ne 0) {
                    throw "client exited with non-zero code: $clientExitCode"
                }
                break
            }
        } else {
            break
        }

        try {
            foreach ($p in $NodeProcs) {
                if ($null -eq $p) { continue }
                try { $p.Refresh() } catch {}
                if ($p.HasExited) {
                    $nodeExit = 0
                    try { $nodeExit = [int]$p.ExitCode } catch { $nodeExit = 0 }
                    if ($null -eq $nodeExit) { $nodeExit = 0 }
                    if ($nodeExit -ne 0) {
                        throw "node process exited unexpectedly: pid=$($p.Id) exit=$nodeExit"
                    }
                }
            }
            if ((Get-Date) -gt $deadline) {
                throw "run timed out after $RunTimeoutSec seconds"
            }
        } catch {
            Stop-RunProcesses -NodeProcs $NodeProcs -ClientProc $ClientProc
            throw
        }
        Start-Sleep -Seconds 2
    }

    foreach ($p in $NodeProcs) {
        if ($null -eq $p) { continue }
        try {
            $p.Refresh()
            if ($p.HasExited) {
                $nodeExit = 0
                try { $nodeExit = [int]$p.ExitCode } catch { $nodeExit = 0 }
                if ($null -eq $nodeExit) { $nodeExit = 0 }
                if ($nodeExit -ne 0) {
                    throw "node process exited with non-zero code: pid=$($p.Id) exit=$nodeExit"
                }
                continue
            }
            Wait-Process -Id $p.Id -Timeout 30 -ErrorAction Stop
            $p.Refresh()
            $nodeExit = 0
            try { $nodeExit = [int]$p.ExitCode } catch { $nodeExit = 0 }
            if ($null -eq $nodeExit) { $nodeExit = 0 }
            if ($nodeExit -ne 0) {
                throw "node process exited with non-zero code: pid=$($p.Id) exit=$nodeExit"
            }
        } catch {
            Stop-RunProcesses -NodeProcs $NodeProcs -ClientProc $null
            throw
        }
    }
}

$repo = [System.IO.Path]::GetFullPath($RepoRoot)
$datasetPath = Resolve-PathSafe $repo $Dataset
if (-not (Test-Path $datasetPath)) {
    throw "Dataset file not found: $datasetPath"
}

$outRootPath = Resolve-PathSafe $repo $OutRoot
New-Item -ItemType Directory -Path $outRootPath -Force | Out-Null
$metricsOutRoot = Resolve-PathSafe $repo $MetricsOutDir
New-Item -ItemType Directory -Path $metricsOutRoot -Force | Out-Null

$shardList = @($ShardNums.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" } | ForEach-Object { [int]$_ })
$windowList = @($Windows.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" } | ForEach-Object { [int]$_ })

Write-Host ""
Write-Host "============================================"
Write-Host "Probe matrix runner (Exp2/Exp6)"
Write-Host "RepoRoot            : $repo"
Write-Host "Dataset             : $datasetPath"
Write-Host "Strategy            : $Strategy"
Write-Host "ShardNums           : $($shardList -join ',')"
Write-Host "NodesPerShard       : $NodesPerShard"
Write-Host "Windows(ms)         : $($windowList -join ',')"
Write-Host "Runs                : $Runs"
Write-Host "MaxInjectTxs        : $MaxInjectTxs"
Write-Host "SyncProbeMaxAccounts: $SyncProbeMaxAccounts"
Write-Host "RunTimeoutSec       : $RunTimeoutSec"
Write-Host "OutRoot             : $outRootPath"
Write-Host "MetricsOutDir       : $metricsOutRoot"
Write-Host "============================================"
Write-Host ""

foreach ($shardNum in $shardList) {
    foreach ($window in $windowList) {
        for ($run = 1; $run -le $Runs; $run++) {
            $runDir = Join-Path $outRootPath ("shards{0}_nodes{1}\window_{2}\run{3}" -f $shardNum, $NodesPerShard, $window, $run)
            New-Item -ItemType Directory -Path $runDir -Force | Out-Null

            Write-Host ""
            Write-Host ("---- shards={0} nodes={1} window={2} run={3}/{4} ----" -f $shardNum, $NodesPerShard, $window, $run, $Runs)

            Cleanup-RunState $repo

            $nodeProcs = @()
            for ($s = 0; $s -lt $shardNum; $s++) {
                for ($n = 0; $n -lt $NodesPerShard; $n++) {
                    $nodeStdOut = Join-Path $runDir ("S{0}_N{1}.out.log" -f $s, $n)
                    $nodeStdErr = Join-Path $runDir ("S{0}_N{1}.err.log" -f $s, $n)
                    $nodeArgs = @(
                        "run", "main.go",
                        "-S", $shardNum,
                        "-s", ("S{0}" -f $s),
                        "-f", "0",
                        "-n", ("N{0}" -f $n),
                        "-t", $datasetPath,
                        "-m", $Strategy,
                        "--deltaAggregateWindowMs", $window
                    )
                    $proc = Start-Process -FilePath "go" -ArgumentList $nodeArgs -WorkingDirectory $repo -PassThru -NoNewWindow -RedirectStandardOutput $nodeStdOut -RedirectStandardError $nodeStdErr
                    $nodeProcs += $proc
                }
            }

            Start-Sleep -Seconds $NodeWaitSec

            $clientStdOut = Join-Path $runDir "client.out.log"
            $clientStdErr = Join-Path $runDir "client.err.log"
            $clientArgs = @(
                "run", "main.go",
                "-S", $shardNum,
                "-f", "0",
                "-c",
                "-t", $datasetPath,
                "-m", $Strategy,
                "--enableSyncProbe",
                "--syncProbeMaxAccounts", $SyncProbeMaxAccounts,
                "--maxInjectTxs", $MaxInjectTxs,
                "--deltaAggregateWindowMs", $window
            )
            $clientProc = Start-Process -FilePath "go" -ArgumentList $clientArgs -WorkingDirectory $repo -PassThru -NoNewWindow -RedirectStandardOutput $clientStdOut -RedirectStandardError $clientStdErr

            Wait-And-CleanupProcesses -NodeProcs $nodeProcs -ClientProc $clientProc -RunTimeoutSec $RunTimeoutSec

            $logDir = Join-Path $repo "log"
            if (-not (Test-Path $logDir)) {
                throw "log directory not found after run: $logDir"
            }
            Get-ChildItem $logDir -File -Filter "*.csv" | ForEach-Object {
                Copy-Item $_.FullName (Join-Path $runDir $_.Name) -Force
            }

            $metaPath = Join-Path $runDir "run_meta.txt"
            @(
                "strategy=$Strategy"
                "dataset=$Dataset"
                "shards=$shardNum"
                "nodes_per_shard=$NodesPerShard"
                "delta_window_ms=$window"
                "run_index=$run"
                "max_inject_txs=$MaxInjectTxs"
                "sync_probe_max_accounts=$SyncProbeMaxAccounts"
            ) | Set-Content -Path $metaPath -Encoding UTF8

            $metricsOut = Join-Path $metricsOutRoot ("shards{0}_nodes{1}_window{2}_run{3}.json" -f $shardNum, $NodesPerShard, $window, $run)
            $metricsOutTagged = Join-Path $metricsOutRoot ("shards{0}_nodes{1}_window{2}_run{3}_probe{4}_inject{5}.json" -f $shardNum, $NodesPerShard, $window, $run, $SyncProbeMaxAccounts, $MaxInjectTxs)
            try {
                & python "scripts\metrics_definitions.py" --log-dir "log" --out $metricsOut | Out-Null
                Copy-Item $metricsOut $metricsOutTagged -Force
            } catch {
                Write-Host "[WARN] metrics_definitions.py failed, skip metrics JSON for this run."
            }

            $csvCount = (Get-ChildItem $runDir -File -Filter "*.csv" | Measure-Object).Count
            Write-Host "Run done. Copied CSV files: $csvCount"
        }
    }
}

Write-Host ""
Write-Host "[DONE] Probe matrix runs finished."
Write-Host "Output directory: $outRootPath"
Write-Host "Metrics directory: $metricsOutRoot"
Write-Host ""

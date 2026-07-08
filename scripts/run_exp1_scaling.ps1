param(
    [string]$RepoRoot = ".",
    [string]$Dataset = "selectedTxs_300K.csv",
    [string]$Strategies = "SOTA-Lock,Fine-tuned-Lock,MVSS,MVSS-Delta",
    [int]$ShardNum = 4,
    [string]$ShardNums = "",
    [int]$NodesPerShard = 4,
    [int]$Runs = 3,
    [int]$RunStart = 1,
    [int]$NodeWaitSec = 12,
    [int]$MaxInjectTxs = 50000,
    [int]$InjectSpeed = 400,
    [string]$InjectSpeeds = "",
    [int]$DeltaWindowMs = 200,
    [int]$RunTimeoutSec = 1200,
    [string]$ExpRoot = "results/exp1_scaling",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if ($env:DRY_RUN -eq "1") {
    $DryRun = $true
}

function Resolve-PathSafe([string]$Base, [string]$Relative) {
    return [System.IO.Path]::GetFullPath((Join-Path $Base $Relative))
}

function Sanitize-PathToken([string]$v) {
    if ([string]::IsNullOrWhiteSpace($v)) { return "unknown" }
    $x = $v -replace "[^a-zA-Z0-9_.-]", "_"
    return $x.Trim("_")
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

function Wait-And-CleanupProcesses(
    [System.Diagnostics.Process[]]$NodeProcs,
    [System.Diagnostics.Process]$ClientProc,
    [int]$TimeoutSec
) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
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
                throw "run timed out after $TimeoutSec seconds"
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
        } catch {
            Stop-RunProcesses -NodeProcs $NodeProcs -ClientProc $null
            throw
        }
    }
}

function Stop-StaleExperimentProcesses {
    foreach ($name in @("main", "go")) {
        Get-Process -Name $name -ErrorAction SilentlyContinue |
            Stop-Process -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
}

function Get-TxCommittedTotal([string]$RunDir) {
    $total = 0
    Get-ChildItem $RunDir -File -Filter "S*_block.csv" -ErrorAction SilentlyContinue | ForEach-Object {
        $rows = Import-Csv $_.FullName -ErrorAction SilentlyContinue
        foreach ($row in $rows) {
            if ($null -ne $row.tx_committed -and $row.tx_committed -ne "") {
                $total += [int]$row.tx_committed
            }
        }
    }
    return $total
}

function Test-RunAcceptance([string]$ClientLogPath, [int]$ExpectedTxCommitted) {
    if (-not (Test-Path $ClientLogPath)) {
        return @{
            ok = $false
            reason = "client.out.log missing"
        }
    }
    $fileInfo = Get-Item $ClientLogPath
    if ($fileInfo.Length -lt 200) {
        return @{
            ok = $false
            reason = "client.out.log too short ($($fileInfo.Length) bytes)"
        }
    }
    $text = Get-Content -Path $ClientLogPath -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($text)) {
        return @{
            ok = $false
            reason = "client.out.log empty"
        }
    }
    $checks = @(
        @{ name = "migrate_wanted"; pattern = "MigrateWanted" }
        @{ name = "pending_done"; pattern = "pending" }
        @{ name = "graceful_stop"; pattern = "emptyStreakByShard" }
    )
    $missing = @()
    foreach ($c in $checks) {
        if ($text -notmatch $c.pattern) {
            $missing += $c.name
        }
    }
    if ($missing.Count -gt 0) {
        return @{
            ok = $false
            reason = ("missing: " + ($missing -join ", "))
        }
    }
    if ($text -match "SyncProbe enabled") {
        return @{
            ok = $false
            reason = "SyncProbe should be disabled for Exp1"
        }
    }
    return @{
        ok = $true
        reason = "ok"
    }
}

function Get-RunDirPath(
    [string]$RawRoot,
    [int]$ShardNum,
    [int]$NodesPerShard,
    [int]$InjectSpeed,
    [int]$MaxInjectTxs,
    [string]$StrategyToken,
    [int]$RunIndex
) {
    return Join-Path $RawRoot (
        "shards{0}_nodes{1}\speed_{2}\maxinj_{3}\strategy_{4}\run{5}" -f `
            $ShardNum, $NodesPerShard, $InjectSpeed, $MaxInjectTxs, $StrategyToken, $RunIndex
    )
}

function Parse-IntCsvOrDefault(
    [string]$CsvValue,
    [int]$DefaultValue
) {
    $out = @()
    if (-not [string]::IsNullOrWhiteSpace($CsvValue)) {
        foreach ($item in $CsvValue.Split(",")) {
            $t = $item.Trim()
            if ($t -eq "") { continue }
            $parsed = 0
            if ([int]::TryParse($t, [ref]$parsed)) {
                $out += $parsed
            } else {
                throw "Invalid integer token in csv list: '$t'"
            }
        }
    }
    if ($out.Count -eq 0) {
        $out = @($DefaultValue)
    }
    return $out
}

$repo = [System.IO.Path]::GetFullPath($RepoRoot)
$datasetPath = Resolve-PathSafe $repo $Dataset
if (-not (Test-Path $datasetPath)) {
    throw "Dataset file not found: $datasetPath"
}

$expRootPath = Resolve-PathSafe $repo $ExpRoot
$rawRootPath = Join-Path $expRootPath "raw"
$metricsRootPath = Join-Path $expRootPath "metrics"
$summaryRootPath = Join-Path $expRootPath "summary"
New-Item -ItemType Directory -Path $rawRootPath -Force | Out-Null
New-Item -ItemType Directory -Path $metricsRootPath -Force | Out-Null
New-Item -ItemType Directory -Path $summaryRootPath -Force | Out-Null

$strategyList = @($Strategies.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
$shardList = @(Parse-IntCsvOrDefault -CsvValue $ShardNums -DefaultValue $ShardNum)
$speedList = @(Parse-IntCsvOrDefault -CsvValue $InjectSpeeds -DefaultValue $InjectSpeed)

if ($MaxInjectTxs -le 0) { $MaxInjectTxs = 50000 }
$maxInjectSpeed = ($speedList | Measure-Object -Maximum).Maximum
if ($null -eq $maxInjectSpeed -or $maxInjectSpeed -le 0) { $maxInjectSpeed = [math]::Max($InjectSpeed, 1) }
# 超时：注入耗时 + 240s drain；正常 MVSS 8×800@50000 约 250–300s 内结束
$minTimeout = [int]([math]::Ceiling($MaxInjectTxs / $maxInjectSpeed + 240))
if ($RunTimeoutSec -lt $minTimeout) { $RunTimeoutSec = $minTimeout }
if ($RunTimeoutSec -gt 360) { $RunTimeoutSec = 360 }

Write-Host ""
Write-Host "============================================"
Write-Host "Exp1 scaling runner (probe OFF)"
Write-Host "RepoRoot       : $repo"
Write-Host "Dataset        : $datasetPath"
Write-Host "Strategies     : $($strategyList -join ', ')"
Write-Host "Shard list     : $($shardList -join ', ')"
Write-Host "Nodes/shard    : $NodesPerShard"
Write-Host "Runs/strategy  : $Runs"
Write-Host "MaxInjectTxs   : $MaxInjectTxs"
Write-Host "RunTimeoutSec  : $RunTimeoutSec"
Write-Host "Speed list     : $($speedList -join ', ')"
Write-Host "DeltaWindowMs  : $DeltaWindowMs"
Write-Host "ExpRoot        : $expRootPath"
Write-Host "DryRun         : $($DryRun.IsPresent)"
Write-Host "============================================"
Write-Host ""

$totalRuns = $strategyList.Count * $Runs * $shardList.Count * $speedList.Count
$runCounter = 0
$passCount = 0
$failCount = 0

foreach ($currShardNum in $shardList) {
    foreach ($currInjectSpeed in $speedList) {
        foreach ($strategy in $strategyList) {
            $strategyToken = Sanitize-PathToken($strategy)
            for ($r = 0; $r -lt $Runs; $r++) {
                $run = $RunStart + $r
                $runCounter++
                $runDir = Get-RunDirPath -RawRoot $rawRootPath -ShardNum $currShardNum -NodesPerShard $NodesPerShard `
                    -InjectSpeed $currInjectSpeed -MaxInjectTxs $MaxInjectTxs -StrategyToken $strategyToken -RunIndex $run

                Write-Host ""
                Write-Host ("---- [{0}/{1}] shards={2} speed={3} strategy={4} run={5} (batch {6}/{7}) ----" -f `
                    $runCounter, $totalRuns, $currShardNum, $currInjectSpeed, $strategy, $run, ($r + 1), $Runs)
                Write-Host "Out: $runDir"

                if ($DryRun.IsPresent) {
                    Write-Host "[DRY-RUN] skip execution"
                    continue
                }

                Stop-StaleExperimentProcesses
                New-Item -ItemType Directory -Path $runDir -Force | Out-Null
                Cleanup-RunState $repo

                $nodeProcs = @()
                for ($s = 0; $s -lt $currShardNum; $s++) {
                    for ($n = 0; $n -lt $NodesPerShard; $n++) {
                        $nodeStdOut = Join-Path $runDir ("S{0}_N{1}.out.log" -f $s, $n)
                        $nodeStdErr = Join-Path $runDir ("S{0}_N{1}.err.log" -f $s, $n)
                        $nodeArgs = @(
                            "run", "main.go",
                            "-S", $currShardNum,
                            "-s", ("S{0}" -f $s),
                            "-f", "0",
                            "-n", ("N{0}" -f $n),
                            "-t", $datasetPath,
                            "-m", $strategy
                        )
                        if ($strategy -eq "MVSS-Delta") {
                            $nodeArgs += @("--deltaAggregateWindowMs", $DeltaWindowMs)
                        }
                        $proc = Start-Process -FilePath "go" -ArgumentList $nodeArgs -WorkingDirectory $repo -PassThru -NoNewWindow `
                            -RedirectStandardOutput $nodeStdOut -RedirectStandardError $nodeStdErr
                        $nodeProcs += $proc
                    }
                }

                Start-Sleep -Seconds $NodeWaitSec

                $clientStdOut = Join-Path $runDir "client.out.log"
                $clientStdErr = Join-Path $runDir "client.err.log"
                $clientArgs = @(
                    "run", "main.go",
                    "-S", $currShardNum,
                    "-f", "0",
                    "-c",
                    "-t", $datasetPath,
                    "-m", $strategy,
                    "--maxInjectTxs", $MaxInjectTxs,
                    "--injectSpeed", $currInjectSpeed
                )
                if ($strategy -eq "MVSS-Delta") {
                    $clientArgs += @("--deltaAggregateWindowMs", $DeltaWindowMs)
                }

                $startedAt = Get-Date
                $clientProc = Start-Process -FilePath "go" -ArgumentList $clientArgs -WorkingDirectory $repo -PassThru -NoNewWindow `
                    -RedirectStandardOutput $clientStdOut -RedirectStandardError $clientStdErr

                try {
                    Wait-And-CleanupProcesses -NodeProcs $nodeProcs -ClientProc $clientProc -TimeoutSec $RunTimeoutSec
                } catch {
                    $elapsedFail = [int]((Get-Date) - $startedAt).TotalSeconds
                    Stop-RunProcesses -NodeProcs $nodeProcs -ClientProc $clientProc
                    Cleanup-RunState $repo
                    @(
                        "status=fail"
                        "reason=$($_.Exception.Message)"
                        "elapsed_sec=$elapsedFail"
                    ) | Set-Content -Path (Join-Path $runDir "run_status.txt") -Encoding UTF8
                    $failCount++
                    Write-Host "[FAIL] $($_.Exception.Message)"
                    Start-Sleep -Seconds 5
                    continue
                }

                $elapsedSec = [int]((Get-Date) - $startedAt).TotalSeconds

                $logDir = Join-Path $repo "log"
                if (-not (Test-Path $logDir)) {
                    throw "log directory not found after run: $logDir"
                }
                Get-ChildItem $logDir -File -Filter "*.csv" | ForEach-Object {
                    Copy-Item $_.FullName (Join-Path $runDir $_.Name) -Force
                }

                $accept = Test-RunAcceptance -ClientLogPath $clientStdOut -ExpectedTxCommitted $MaxInjectTxs
                $csvCount = (Get-ChildItem $runDir -File -Filter "*.csv" | Measure-Object).Count
                $txCommitted = Get-TxCommittedTotal -RunDir $runDir

                @(
                    "strategy=$strategy"
                    "dataset=$Dataset"
                    "shard_num=$currShardNum"
                    "nodes_per_shard=$NodesPerShard"
                    "inject_speed=$currInjectSpeed"
                    "max_inject_txs=$MaxInjectTxs"
                    "enable_sync_probe=false"
                    "delta_window_ms=$(if ($strategy -eq 'MVSS-Delta') { $DeltaWindowMs } else { 0 })"
                    "run_index=$run"
                    "elapsed_sec=$elapsedSec"
                    "csv_count=$csvCount"
                    "tx_committed_total=$txCommitted"
                ) | Set-Content -Path (Join-Path $runDir "run_meta.txt") -Encoding UTF8

                $txMinOk = [math]::Max(1, $MaxInjectTxs - 30)
                $txOk = ($txCommitted -ge $txMinOk)
                $status = if ($accept.ok -and $csvCount -ge 4 -and $txOk) { "pass" } else {
                    if (-not $accept.ok) { $accept.reason }
                    elseif ($csvCount -lt 4) { "insufficient csv ($csvCount)" }
                    elseif (-not $txOk) { "tx_committed=$txCommitted expected>=$txMinOk" }
                    else { "unknown" }
                }
                $isPass = ($status -eq "pass")
                if ($isPass) { $passCount++ } else { $failCount++ }

                @(
                    "status=$(if ($isPass) { 'pass' } else { 'fail' })"
                    "reason=$status"
                    "elapsed_sec=$elapsedSec"
                    "csv_count=$csvCount"
                ) | Set-Content -Path (Join-Path $runDir "run_status.txt") -Encoding UTF8

                $metricsOut = Join-Path $metricsRootPath (
                    "shards{0}_nodes{1}_speed{2}_inject{3}_{4}_run{5}.json" -f `
                        $currShardNum, $NodesPerShard, $currInjectSpeed, $MaxInjectTxs, $strategyToken, $run
                )
                try {
                    & python (Join-Path $repo "scripts\metrics_definitions.py") --log-dir "log" --out $metricsOut | Out-Null
                    Copy-Item $metricsOut (Join-Path $runDir "metrics.json") -Force
                } catch {
                    Write-Host "[WARN] metrics_definitions.py failed for this run."
                }

                Write-Host ("Run finished in {0}s. acceptance={1} csv={2} tx_committed={3}" -f $elapsedSec, $(if ($isPass) { "PASS" } else { "FAIL" }), $csvCount, $txCommitted)
                Start-Sleep -Seconds 3
            }
        }
    }
}

Write-Host ""
Write-Host "[DONE] Exp1 runner finished."
Write-Host "Total=$totalRuns pass=$passCount fail=$failCount"
Write-Host "Exp root : $expRootPath"
Write-Host ""

if (-not $DryRun.IsPresent -and $failCount -gt 0) {
    exit 1
}

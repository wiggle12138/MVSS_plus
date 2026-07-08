param(
    [string]$RepoRoot = ".",
    [string]$Dataset = "data/exp4/13000000to13249999_BlockTransaction_head150k.csv",
    [string]$DatasetToken = "eth_head150k",
    [string]$Strategies = "SOTA-Lock,Fine-tuned-Lock,MVSS,MVSS-Delta",
    [int]$ShardNum = 8,
    [int]$NodesPerShard = 4,
    [int]$Runs = 3,
    [int]$RunStart = 1,
    [int]$NodeWaitSec = 60,
    [int]$MaxInjectTxs = 50000,
    [int]$InjectSpeed = 800,
    [int]$DeltaWindowMs = 200,
    [int]$RunTimeoutSec = 1200,
    [string]$ExpRoot = "results/exp4_eth_workload",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if ($env:DRY_RUN -eq "1") { $DryRun = $true }

function Resolve-PathSafe([string]$Base, [string]$Relative) {
    return [System.IO.Path]::GetFullPath((Join-Path $Base $Relative))
}

function Sanitize-PathToken([string]$v) {
    if ([string]::IsNullOrWhiteSpace($v)) { return "unknown" }
    return ($v -replace "[^a-zA-Z0-9_.-]", "_").Trim("_")
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
                $code = 0
                try { $code = [int]$ClientProc.ExitCode } catch {}
                if ($null -eq $code) { $code = 0 }
                if ($code -ne 0) { throw "client exited with non-zero code: $code" }
                break
            }
        } else { break }
        foreach ($p in $NodeProcs) {
            if ($null -eq $p) { continue }
            try { $p.Refresh() } catch {}
            if ($p.HasExited) {
                $ne = 0
                try { $ne = [int]$p.ExitCode } catch {}
                if ($null -eq $ne) { $ne = 0 }
                if ($ne -ne 0) { throw "node exited unexpectedly: pid=$($p.Id) exit=$ne" }
            }
        }
        if ((Get-Date) -gt $deadline) { throw "run timed out after $TimeoutSec seconds" }
        Start-Sleep -Seconds 2
    }
    foreach ($p in $NodeProcs) {
        if ($null -eq $p) { continue }
        try {
            $p.Refresh()
            if (-not $p.HasExited) { Wait-Process -Id $p.Id -Timeout 180 -ErrorAction Stop }
        } catch {
            Stop-RunProcesses -NodeProcs $NodeProcs -ClientProc $null
            throw
        }
    }
}

function Stop-StaleExperimentProcesses {
    foreach ($name in @("main", "go")) {
        Get-Process -Name $name -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
}

function Get-TxCommittedTotal([string]$RunDir) {
    $total = 0
    Get-ChildItem $RunDir -File -Filter "S*_block.csv" -ErrorAction SilentlyContinue | ForEach-Object {
        Import-Csv $_.FullName -ErrorAction SilentlyContinue | ForEach-Object {
            if ($null -ne $_.tx_committed -and $_.tx_committed -ne "") { $total += [int]$_.tx_committed }
        }
    }
    return $total
}

function Test-RunAcceptance([string]$ClientLogPath, [int]$ExpectedTxCommitted) {
    if (-not (Test-Path $ClientLogPath)) { return @{ ok = $false; reason = "client.out.log missing" } }
    if ((Get-Item $ClientLogPath).Length -lt 200) { return @{ ok = $false; reason = "client.out.log too short" } }
    $text = Get-Content -Path $ClientLogPath -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($text)) { return @{ ok = $false; reason = "client.out.log empty" } }
    foreach ($pat in @("MigrateWanted", "pending", "emptyStreakByShard")) {
        if ($text -notmatch $pat) { return @{ ok = $false; reason = "missing: $pat" } }
    }
    if ($text -match "SyncProbe enabled") { return @{ ok = $false; reason = "SyncProbe should be disabled for Exp4" } }
    return @{ ok = $true; reason = "ok" }
}

$repo = [System.IO.Path]::GetFullPath($RepoRoot)
$datasetPath = Resolve-PathSafe $repo $Dataset
if (-not (Test-Path $datasetPath)) { throw "Dataset not found: $datasetPath" }

if ($MaxInjectTxs -le 0) { $MaxInjectTxs = 50000 }
if ($InjectSpeed -le 0) { $InjectSpeed = 800 }
$minTimeout = [int]([math]::Ceiling($MaxInjectTxs / [math]::Max($InjectSpeed, 1) * 2.0 + 600))
if ($RunTimeoutSec -lt $minTimeout) { $RunTimeoutSec = $minTimeout }

$expRootPath = Resolve-PathSafe $repo $ExpRoot
$rawRootPath = Join-Path $expRootPath "raw"
$metricsRootPath = Join-Path $expRootPath "metrics"
$summaryRootPath = Join-Path $expRootPath "summary"
New-Item -ItemType Directory -Path $rawRootPath,$metricsRootPath,$summaryRootPath -Force | Out-Null

$datasetTokenSafe = Sanitize-PathToken($DatasetToken)
$strategyList = @($Strategies.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
$totalRuns = $strategyList.Count * $Runs
$runCounter = 0
$passCount = 0
$failCount = 0

Write-Host ""
Write-Host "============================================"
Write-Host "Exp4 Ethereum workload (probe OFF)"
Write-Host "Dataset        : $datasetPath"
Write-Host "Dataset token  : $datasetTokenSafe"
Write-Host "Strategies     : $($strategyList -join ', ')"
Write-Host "Shards/Nodes   : $ShardNum / $NodesPerShard"
Write-Host "Runs/strategy  : $Runs"
Write-Host "MaxInjectTxs   : $MaxInjectTxs"
Write-Host "InjectSpeed    : $InjectSpeed"
Write-Host "RunTimeoutSec  : $RunTimeoutSec"
Write-Host "ExpRoot        : $expRootPath"
Write-Host "DryRun         : $($DryRun.IsPresent)"
Write-Host "============================================"
Write-Host ""

foreach ($strategy in $strategyList) {
    $strategyToken = Sanitize-PathToken($strategy)
    for ($r = 0; $r -lt $Runs; $r++) {
        $run = $RunStart + $r
        $runCounter++
        $runDir = Join-Path $rawRootPath (
            "dataset_{0}\shards{1}_nodes{2}\strategy_{3}\run{4}" -f $datasetTokenSafe, $ShardNum, $NodesPerShard, $strategyToken, $run
        )

        Write-Host ""
        Write-Host ("---- [{0}/{1}] strategy={2} run={3} ----" -f $runCounter, $totalRuns, $strategy, $run)
        Write-Host "Out: $runDir"

        if ($DryRun.IsPresent) {
            Write-Host "[DRY-RUN] skip execution"
            continue
        }

        Stop-StaleExperimentProcesses
        New-Item -ItemType Directory -Path $runDir -Force | Out-Null
        Cleanup-RunState $repo

        $nodeProcs = @()
        for ($s = 0; $s -lt $ShardNum; $s++) {
            for ($n = 0; $n -lt $NodesPerShard; $n++) {
                $nodeStdOut = Join-Path $runDir ("S{0}_N{1}.out.log" -f $s, $n)
                $nodeStdErr = Join-Path $runDir ("S{0}_N{1}.err.log" -f $s, $n)
                $nodeArgs = @("run", "main.go", "-S", $ShardNum, "-s", ("S{0}" -f $s), "-f", "0", "-n", ("N{0}" -f $n), "-t", $datasetPath, "-m", $strategy)
                if ($strategy -eq "MVSS-Delta") { $nodeArgs += @("--deltaAggregateWindowMs", $DeltaWindowMs) }
                $nodeProcs += Start-Process -FilePath "go" -ArgumentList $nodeArgs -WorkingDirectory $repo -PassThru -NoNewWindow `
                    -RedirectStandardOutput $nodeStdOut -RedirectStandardError $nodeStdErr
            }
        }

        Start-Sleep -Seconds $NodeWaitSec

        $clientStdOut = Join-Path $runDir "client.out.log"
        $clientStdErr = Join-Path $runDir "client.err.log"
        $clientArgs = @("run", "main.go", "-S", $ShardNum, "-f", "0", "-c", "-t", $datasetPath, "-m", $strategy, `
            "--maxInjectTxs", $MaxInjectTxs, "--injectSpeed", $InjectSpeed)
        if ($strategy -eq "MVSS-Delta") { $clientArgs += @("--deltaAggregateWindowMs", $DeltaWindowMs) }

        $startedAt = Get-Date
        $clientProc = Start-Process -FilePath "go" -ArgumentList $clientArgs -WorkingDirectory $repo -PassThru -NoNewWindow `
            -RedirectStandardOutput $clientStdOut -RedirectStandardError $clientStdErr

        try {
            Wait-And-CleanupProcesses -NodeProcs $nodeProcs -ClientProc $clientProc -TimeoutSec $RunTimeoutSec
        } catch {
            Stop-RunProcesses -NodeProcs $nodeProcs -ClientProc $clientProc
            Cleanup-RunState $repo
            @("status=fail", "reason=$($_.Exception.Message)") | Set-Content (Join-Path $runDir "run_status.txt") -Encoding UTF8
            $failCount++
            Write-Host "[FAIL] $($_.Exception.Message)"
            Start-Sleep -Seconds 5
            continue
        }

        $elapsedSec = [int]((Get-Date) - $startedAt).TotalSeconds
        $logDir = Join-Path $repo "log"
        if (-not (Test-Path $logDir)) { throw "log directory missing: $logDir" }
        Get-ChildItem $logDir -File -Filter "*.csv" | ForEach-Object { Copy-Item $_.FullName (Join-Path $runDir $_.Name) -Force }

        $accept = Test-RunAcceptance -ClientLogPath $clientStdOut -ExpectedTxCommitted $MaxInjectTxs
        $csvCount = (Get-ChildItem $runDir -File -Filter "*.csv" | Measure-Object).Count
        $txCommitted = Get-TxCommittedTotal -RunDir $runDir

        @(
            "strategy=$strategy", "dataset=$Dataset", "dataset_token=$datasetTokenSafe"
            "shard_num=$ShardNum", "nodes_per_shard=$NodesPerShard"
            "inject_speed=$InjectSpeed", "max_inject_txs=$MaxInjectTxs"
            "enable_sync_probe=false"
            "delta_window_ms=$(if ($strategy -eq 'MVSS-Delta') { $DeltaWindowMs } else { 0 })"
            "run_index=$run", "elapsed_sec=$elapsedSec"
            "csv_count=$csvCount", "tx_committed_total=$txCommitted"
        ) | Set-Content (Join-Path $runDir "run_meta.txt") -Encoding UTF8

        $txMinOk = [math]::Max(1, $MaxInjectTxs - 30)
        $isPass = $accept.ok -and $csvCount -ge 4 -and ($txCommitted -ge $txMinOk)
        if (-not $accept.ok) { $reason = $accept.reason }
        elseif ($csvCount -lt 4) { $reason = "insufficient csv ($csvCount)" }
        elseif ($txCommitted -lt $txMinOk) { $reason = "tx_committed=$txCommitted expected>=$txMinOk" }
        else { $reason = "ok" }
        if ($isPass) { $passCount++ } else { $failCount++ }

        @(
            "status=$(if ($isPass) { 'pass' } else { 'fail' })"
            "reason=$reason", "elapsed_sec=$elapsedSec", "csv_count=$csvCount"
        ) | Set-Content (Join-Path $runDir "run_status.txt") -Encoding UTF8

        $metricsOut = Join-Path $metricsRootPath (
            "dataset_{0}_shards{1}_nodes{2}_{3}_run{4}.json" -f $datasetTokenSafe, $ShardNum, $NodesPerShard, $strategyToken, $run
        )
        try {
            & python (Join-Path $repo "scripts\metrics_definitions.py") --log-dir $runDir --out $metricsOut | Out-Null
            Copy-Item $metricsOut (Join-Path $runDir "metrics.json") -Force
        } catch {
            Write-Host "[WARN] metrics_definitions.py failed."
        }

        Write-Host ("Run {0}s acceptance={1} csv={2} tx={3}" -f $elapsedSec, $(if ($isPass){"PASS"}else{"FAIL"}), $csvCount, $txCommitted)
        Start-Sleep -Seconds 3
    }
}

Write-Host ""
Write-Host "[DONE] Exp4 finished. total=$totalRuns pass=$passCount fail=$failCount"
Write-Host "Exp root: $expRootPath"
if (-not $DryRun.IsPresent -and $failCount -gt 0) { exit 1 }

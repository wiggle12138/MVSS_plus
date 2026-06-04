param(
    [Parameter(Mandatory = $true)]
    [string]$LogFile,
    [Parameter(Mandatory = $true)]
    [string]$WorkDir,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$GoArgs
)

$ErrorActionPreference = "Continue"

# go pipe stdout is UTF-8; match console + log file
$utf8 = [System.Text.UTF8Encoding]::new($false)
try { chcp 65001 | Out-Null } catch {}
[Console]::OutputEncoding = $utf8
[Console]::InputEncoding = $utf8
$OutputEncoding = $utf8

Set-Location -LiteralPath $WorkDir

$logDir = Split-Path -Parent $LogFile
if ($logDir -and -not (Test-Path -LiteralPath $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

$writer = [System.IO.StreamWriter]::new($LogFile, $false, $utf8)
try {
    & go @GoArgs 2>&1 | ForEach-Object {
        $line = if ($_ -is [string]) { $_ } else { $_.ToString() }
        [Console]::WriteLine($line)
        $writer.WriteLine($line)
        $writer.Flush()
    }
} finally {
    $writer.Dispose()
}

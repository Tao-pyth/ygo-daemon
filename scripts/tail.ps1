param(
    [string]$Cmd,
    [int]$Tail = 200
)

$ErrorActionPreference = 'Stop'

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$logsRoot = Join-Path $repoRoot 'data/logs'
if ($Cmd) {
    $logsRoot = Join-Path $logsRoot $Cmd
}

if (-not (Test-Path $logsRoot)) {
    Write-Error "log directory not found: $logsRoot"
    exit 1
}

$target = Get-ChildItem -Path $logsRoot -Filter '*.log' -File -Recurse |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1

if (-not $target) {
    Write-Error "no log files found under: $logsRoot"
    exit 1
}

Write-Host "[tail] selected log: $($target.FullName)"
$firstLine = Get-Content -Path $target.FullName -TotalCount 1 -ErrorAction SilentlyContinue
if ($firstLine) {
    Write-Host "[tail] header: $firstLine"
}

Get-Content -Path $target.FullName -Wait -Tail $Tail

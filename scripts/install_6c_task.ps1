[CmdletBinding()]
param(
    [string]$TaskName = "ResearchAssistant-EOD",
    [string]$ProjectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonPath = "python",
    [string]$Watchlist = "default",
    [ValidatePattern('^([01][0-9]|2[0-3]):[0-5][0-9]$')]
    [string]$ScheduleTime = "18:00",
    [ValidateRange(0, 1440)]
    [int]$GraceMinutes = 30,
    [ValidateSet("twse", "mock")]
    [string]$Provider = "twse"
)

$resolvedRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path
$schedulerScript = Join-Path $resolvedRoot "scripts\scheduler.py"
$xmlPath = Join-Path $env:TEMP ("ira-6c-" + [guid]::NewGuid().ToString("N") + ".xml")

try {
    & $PythonPath $schedulerScript task-xml `
        --output $xmlPath `
        --project-root $resolvedRoot `
        --task-name $TaskName `
        --python-path $PythonPath `
        --watchlist $Watchlist `
        --provider $Provider `
        --schedule-time $ScheduleTime `
        --grace-minutes $GraceMinutes
    if ($LASTEXITCODE -ne 0) {
        throw "Task XML generation failed"
    }

    & schtasks.exe /Create /TN $TaskName /XML $xmlPath /F | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Windows Task Scheduler registration failed"
    }
    Write-Output "Task Scheduler task registered: $TaskName"
}
finally {
    if (Test-Path -LiteralPath $xmlPath) {
        Remove-Item -LiteralPath $xmlPath -Force
    }
}

$ErrorActionPreference = "Stop"

$dashboardRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$dashboardScript = Join-Path $dashboardRoot "dashboards\Dashboard_Final.py"
$port = 8501
$url = "http://localhost:$port"
$logDir = Join-Path $dashboardRoot "logs"
$stdoutLog = Join-Path $logDir "streamlit-dashboard.out.log"
$stderrLog = Join-Path $logDir "streamlit-dashboard.err.log"
$canonicalScript = (Resolve-Path -LiteralPath $dashboardScript).Path

New-Item -ItemType Directory -Path $logDir -Force | Out-Null

if (-not (Test-Path -LiteralPath $dashboardScript)) {
    throw "Dashboard script not found: $dashboardScript"
}

function Get-PortListener {
    param([int]$LocalPort)

    Get-NetTCPConnection -LocalPort $LocalPort -State Listen -ErrorAction SilentlyContinue |
        Select-Object -First 1
}

function Get-ProcessCommandLine {
    param([int]$ProcessId)

    try {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction Stop
        return [string]$proc.CommandLine
    } catch {
        return ""
    }
}

function Test-StreamlitHttp {
    param([string]$TargetUrl)

    try {
        $response = Invoke-WebRequest -Uri $TargetUrl -UseBasicParsing -TimeoutSec 3
        return $response.StatusCode -ge 200 -and $response.StatusCode -lt 500
    } catch {
        return $false
    }
}

$listener = Get-PortListener -LocalPort $port
if ($listener) {
    $commandLine = Get-ProcessCommandLine -ProcessId $listener.OwningProcess
    $normalizedCommand = $commandLine.ToLowerInvariant()
    $normalizedCanonical = $canonicalScript.ToLowerInvariant()

    if ($normalizedCommand.Contains($normalizedCanonical)) {
        Start-Process $url
        exit 0
    }

    if ($normalizedCommand.Contains("streamlit") -and $normalizedCommand.Contains("dashboard_final.py")) {
        Stop-Process -Id $listener.OwningProcess -Force
        Start-Sleep -Seconds 2
    } else {
        throw "Port $port is already in use by process $($listener.OwningProcess), but it is not the canonical OPTIC dashboard. Command line: $commandLine"
    }
}

if (-not (Get-PortListener -LocalPort $port)) {
    $env:OPTIC_DASHBOARD_CONFIG_DIR = Join-Path $dashboardRoot "config"
    $env:OPTIC_DASHBOARD_OUTPUT_ROOT = Join-Path $dashboardRoot "output"
    $pythonExe = (Get-Command python -ErrorAction Stop).Source

    Start-Process `
        -FilePath $pythonExe `
        -ArgumentList @(
            "-m", "streamlit", "run", "`"$dashboardScript`"",
            "--server.address", "localhost",
            "--server.port", "$port",
            "--server.headless", "true",
            "--browser.gatherUsageStats", "false"
        ) `
        -WorkingDirectory $dashboardRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutLog `
        -RedirectStandardError $stderrLog | Out-Null
}

$deadline = (Get-Date).AddSeconds(45)
while ((Get-Date) -lt $deadline) {
    if (Test-StreamlitHttp -TargetUrl $url) {
        Start-Process $url
        exit 0
    }
    Start-Sleep -Seconds 1
}

Start-Process $url

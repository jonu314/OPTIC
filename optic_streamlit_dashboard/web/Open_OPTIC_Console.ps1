# Launches the OPTIC Console HTML dashboard (HTML UI + thin Snowflake API server).
# Reuses Dashboard_Final.py logic without modifying it. Opens http://localhost:8520.
$ErrorActionPreference = "Stop"
$webDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$server = Join-Path $webDir "optic_console_server.py"
$port = 8520
$url = "http://localhost:$port"
$logDir = Join-Path $webDir "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$out = Join-Path $logDir "optic-console.out.log"
$err = Join-Path $logDir "optic-console.err.log"

function Test-Up { try { (Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3).StatusCode -eq 200 } catch { $false } }

if (Test-Up) { Start-Process $url; exit 0 }

$candidates = @()
if ($env:OPTIC_CONSOLE_PYTHON) { $candidates += $env:OPTIC_CONSOLE_PYTHON }
$defaultPython = Get-Command python -ErrorAction SilentlyContinue
if ($defaultPython) { $candidates += $defaultPython.Source }
if (Get-Command py -ErrorAction SilentlyContinue) {
  $candidates += @(& py -0p 2>$null | ForEach-Object {
    if ($_ -match '([A-Za-z]:\\.+python\.exe)\s*$') { $Matches[1].Trim() }
  })
}
$python = $null
foreach ($candidate in ($candidates | Select-Object -Unique)) {
  if (-not (Test-Path -LiteralPath $candidate)) { continue }
  try {
    & $candidate -c "import pandas, streamlit, snowflake.connector, cryptography, plotly, openpyxl" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $python = $candidate; break }
  } catch { continue }
}
if (-not $python) {
  throw "No Python installation has the OPTIC dependencies. Install the dashboard requirements or set OPTIC_CONSOLE_PYTHON to a compatible python.exe."
}
$serverProcess = Start-Process -FilePath $python `
  -ArgumentList @("-u", "`"$server`"", "--port", "$port", "--no-browser") `
  -WorkingDirectory $webDir -WindowStyle Hidden `
  -RedirectStandardOutput $out -RedirectStandardError $err -PassThru

$deadline = (Get-Date).AddSeconds(60)
while ((Get-Date) -lt $deadline) {
  if (Test-Up) { Start-Process $url; exit 0 }
  if ($serverProcess.HasExited) { throw "OPTIC server exited with code $($serverProcess.ExitCode). Check $err" }
  Start-Sleep -Seconds 1
}
throw "OPTIC server did not come up in time. Check logs in $logDir"

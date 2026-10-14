$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

$env:OPTIC_DASHBOARD_CONFIG_DIR = Join-Path $root "config"
$env:OPTIC_DASHBOARD_OUTPUT_ROOT = Join-Path $root "output"

python -m streamlit run "dashboards/Dashboard_Final.py" `
  --server.address "localhost" `
  --server.port 8501 `
  --server.headless true

#!/usr/bin/env sh
set -eu

exec python -m streamlit run dashboards/Dashboard_Final.py \
  --server.address "${STREAMLIT_SERVER_ADDRESS:-0.0.0.0}" \
  --server.port "${STREAMLIT_SERVER_PORT:-8501}" \
  --server.headless true

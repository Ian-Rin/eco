#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root (directory containing this script)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN=${PYTHON_BIN:-python3}

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] Python interpreter '$PYTHON_BIN' not found." >&2
  exit 1
fi

run_python() {
  local script="$1"
  echo "[INFO] Running $script"
  "$PYTHON_BIN" "$script"
}

run_python "fetch_incremental.py"
run_python "ak_repurchase_plans.py_incremental"

echo "[INFO] Starting FastAPI server via uvicorn"
exec uvicorn app_fastapi:app --host 0.0.0.0 --port 8000

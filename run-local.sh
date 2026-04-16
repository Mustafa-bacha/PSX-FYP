#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime"
mkdir -p "$RUNTIME_DIR"

BACKEND_PID_FILE="$RUNTIME_DIR/backend.pid"
SERVER_PID_FILE="$RUNTIME_DIR/server.pid"
CLIENT_PID_FILE="$RUNTIME_DIR/client.pid"

BACKEND_LOG="$RUNTIME_DIR/backend.log"
SERVER_LOG="$RUNTIME_DIR/server.log"
CLIENT_LOG="$RUNTIME_DIR/client.log"

choose_python() {
  local candidates=(
    "$ROOT_DIR/.venv/bin/python"
    "$ROOT_DIR/../.venv/bin/python"
  )

  for py in "${candidates[@]}"; do
    if [[ -x "$py" ]]; then
      echo "$py"
      return 0
    fi
  done

  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi

  echo "ERROR: No Python interpreter found. Expected .venv or python3." >&2
  exit 1
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

start_service() {
  local name="$1"
  local workdir="$2"
  local command="$3"
  local pid_file="$4"
  local log_file="$5"
  local expected_port="$6"

  if [[ -f "$pid_file" ]]; then
    local existing_pid
    existing_pid="$(cat "$pid_file" 2>/dev/null || true)"
    if is_pid_running "$existing_pid"; then
      echo "$name already running (PID $existing_pid)."
      return 0
    fi
  fi

  nohup bash -lc "cd '$workdir' && $command" >"$log_file" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" > "$pid_file"
  sleep 2

  local port_ok="0"
  if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":${expected_port}$"; then
    port_ok="1"
  fi

  if is_pid_running "$pid" && [[ "$port_ok" == "1" ]]; then
    echo "Started $name (PID $pid). Log: $log_file"
  else
    echo "Failed to start $name. Check log: $log_file" >&2
    return 1
  fi
}

PYTHON_BIN="$(choose_python)"

if [[ "${RUN_INIT_DB:-0}" == "1" ]]; then
  echo "Initializing database..."
  (cd "$ROOT_DIR" && "$PYTHON_BIN" backend/init_db.py)
fi

start_service \
  "backend" \
  "$ROOT_DIR" \
  "FLASK_ENV=production '$PYTHON_BIN' backend/app.py" \
  "$BACKEND_PID_FILE" \
  "$BACKEND_LOG" \
  "5000"

start_service \
  "server" \
  "$ROOT_DIR/server" \
  "npm run dev" \
  "$SERVER_PID_FILE" \
  "$SERVER_LOG" \
  "5001"

start_service \
  "client" \
  "$ROOT_DIR/client" \
  "npm run dev -- --host 0.0.0.0" \
  "$CLIENT_PID_FILE" \
  "$CLIENT_LOG" \
  "5173"

echo
echo "Services requested to start. Endpoints:"
echo "- Python backend: http://127.0.0.1:5000/health"
echo "- Node API:       http://127.0.0.1:5001/api/health"
echo "- React client:   http://127.0.0.1:5173"
echo
echo "Logs:"
echo "- $BACKEND_LOG"
echo "- $SERVER_LOG"
echo "- $CLIENT_LOG"

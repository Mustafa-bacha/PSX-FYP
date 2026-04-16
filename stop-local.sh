#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime"

BACKEND_PID_FILE="$RUNTIME_DIR/backend.pid"
SERVER_PID_FILE="$RUNTIME_DIR/server.pid"
CLIENT_PID_FILE="$RUNTIME_DIR/client.pid"

stop_by_pid_file() {
  local name="$1"
  local pid_file="$2"

  if [[ ! -f "$pid_file" ]]; then
    echo "$name: no PID file."
    return 0
  fi

  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"

  if [[ -z "$pid" ]]; then
    rm -f "$pid_file"
    echo "$name: empty PID file removed."
    return 0
  fi

  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
    echo "$name stopped (PID $pid)."
  else
    echo "$name: PID $pid not running."
  fi

  rm -f "$pid_file"
}

stop_by_pid_file "client" "$CLIENT_PID_FILE"
stop_by_pid_file "server" "$SERVER_PID_FILE"
stop_by_pid_file "backend" "$BACKEND_PID_FILE"

kill_port_listeners() {
  local port="$1"
  local pids
  pids="$(ss -ltnp 2>/dev/null | awk -v p=":${port}" '
    $4 ~ p {
      while (match($0, /pid=[0-9]+/)) {
        pid = substr($0, RSTART + 4, RLENGTH - 4)
        print pid
        $0 = substr($0, RSTART + RLENGTH)
      }
    }
  ' | sort -u)"

  if [[ -z "$pids" ]]; then
    return 0
  fi

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" 2>/dev/null || true
    sleep 0.2
    kill -9 "$pid" 2>/dev/null || true
  done <<< "$pids"

  echo "Freed listeners on port $port."
}

# Safety cleanup for stale processes started outside scripts.
pkill -f '/psx-platform/backend/app.py' 2>/dev/null || true
pkill -f '/psx-platform/server/src/index.js' 2>/dev/null || true
pkill -f 'vite/bin/vite.js' 2>/dev/null || true
kill_port_listeners 5000
kill_port_listeners 5001
kill_port_listeners 5173

echo "Local services stop routine complete."

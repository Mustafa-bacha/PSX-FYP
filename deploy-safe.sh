#!/usr/bin/env bash
set -Eeuo pipefail

# Safe deploy script for constrained instances (t3.micro free-tier friendly)
# Usage (on server):
#   bash deploy-safe.sh
# Optional full refresh:
#   DEPLOY_FULL=1 bash deploy-safe.sh

APP_DIR="${APP_DIR:-/home/ubuntu/PSX-FYP}"
BRANCH="${BRANCH:-main}"
DEPLOY_FULL="${DEPLOY_FULL:-0}"
API_HEALTH_URL="${API_HEALTH_URL:-http://127.0.0.1/api/health}"
BACKEND_HEALTH_URL="${BACKEND_HEALTH_URL:-http://127.0.0.1/backend/health}"
ROOT_HEALTH_URL="${ROOT_HEALTH_URL:-http://127.0.0.1/}"
HEALTH_RETRIES="${HEALTH_RETRIES:-30}"
HEALTH_SLEEP_SEC="${HEALTH_SLEEP_SEC:-2}"

log() {
  printf "[%s] %s\n" "$(date +"%Y-%m-%d %H:%M:%S")" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    log "ERROR: missing required command: $1"
    exit 1
  }
}

check_url() {
  local url="$1"
  local i
  for ((i=1; i<=HEALTH_RETRIES; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      log "health OK: $url"
      return 0
    fi
    sleep "$HEALTH_SLEEP_SEC"
  done
  log "health FAILED: $url"
  return 1
}

on_fail() {
  log "Deploy failed. Showing recent service logs for quick diagnosis..."
  sudo systemctl --no-pager --full status psx-server psx-backend nginx || true
  sudo journalctl -u psx-server -u psx-backend -n 80 --no-pager || true
}
trap on_fail ERR

require_cmd git
require_cmd curl
require_cmd systemctl

if [[ ! -d "$APP_DIR/.git" ]]; then
  log "ERROR: APP_DIR does not look like a git repo: $APP_DIR"
  exit 1
fi

cd "$APP_DIR"

log "Syncing code to origin/$BRANCH..."
git fetch --all --prune
git reset --hard "origin/$BRANCH"
log "Deployed commit: $(git rev-parse --short HEAD)"

if [[ "$DEPLOY_FULL" == "1" ]]; then
  log "Running full dependency/build refresh..."
  if [[ -f server/package.json ]]; then
    npm --prefix server install --omit=dev --no-audit --no-fund
  fi
  if [[ -f client/package.json ]]; then
    npm --prefix client install --no-audit --no-fund
    npm --prefix client run build
  fi
fi

log "Restarting services..."
sudo systemctl restart psx-server psx-backend nginx

log "Waiting for API/backend/root health..."
check_url "$API_HEALTH_URL"
check_url "$BACKEND_HEALTH_URL"
check_url "$ROOT_HEALTH_URL"

log "Deploy complete and healthy."

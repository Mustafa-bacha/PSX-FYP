#!/usr/bin/env bash
set -euo pipefail

# Forced fresh deploy for PSX platform on EC2 using AWS EC2 Instance Connect.
#
# What it does:
# 1) Pushes temporary public key via EC2 Instance Connect
# 2) SSH into instance and force-sync repo to origin/<branch>
# 3) Clears runtime + build caches
# 4) Reinstalls deps and rebuilds frontend
# 5) Publishes dist to nginx root (/var/www/psx-platform)
# 6) Restarts nginx + app services
# 7) Verifies deployed commit + health endpoints
#
# Usage:
#   chmod +x scripts/ec2_fresh_deploy.sh
#   ./scripts/ec2_fresh_deploy.sh \
#     --instance-id i-xxxxxxxxxxxxxxxxx \
#     --host 1.2.3.4 \
#     --key-file /tmp/ec2_tmp_key \
#     --region us-east-1 \
#     --branch sync-mustafa

REGION="us-east-1"
INSTANCE_ID=""
HOST=""
SSH_USER="ubuntu"
KEY_FILE=""
PUB_KEY_FILE=""
REPO_PATH="/home/ubuntu/PSX-FYP"
BRANCH="sync-mustafa"
FRONTEND_WWW="/var/www/psx-platform"
SERVER_SERVICE="psx-server.service"
BACKEND_SERVICE="psx-backend.service"
NGINX_SERVICE="nginx"

usage() {
  cat <<'EOF'
Forced fresh deploy for PSX platform on EC2.

Required:
  --instance-id <id>   EC2 instance id (e.g. i-0c8bf...)
  --host <ip-or-dns>   Public IP/DNS used by ssh
  --key-file <path>    Path to private key file

Optional:
  --region <region>            AWS region (default: us-east-1)
  --ssh-user <user>            SSH user (default: ubuntu)
  --repo-path <path>           Repo path on EC2 (default: /home/ubuntu/PSX-FYP)
  --branch <name>              Branch to deploy (default: sync-mustafa)
  --frontend-www <path>        nginx static root (default: /var/www/psx-platform)
  --server-service <name>      Server service (default: psx-server.service)
  --backend-service <name>     Backend service (default: psx-backend.service)
  --nginx-service <name>       nginx service (default: nginx)
  -h, --help                   Show help

Example:
  ./scripts/ec2_fresh_deploy.sh \
    --instance-id i-0c8bf092e2058a72f \
    --host 3.228.6.178 \
    --key-file /tmp/ec2_tmp_key \
    --region us-east-1 \
    --branch sync-mustafa
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance-id)
      INSTANCE_ID="$2"; shift 2 ;;
    --host)
      HOST="$2"; shift 2 ;;
    --key-file)
      KEY_FILE="$2"; shift 2 ;;
    --region)
      REGION="$2"; shift 2 ;;
    --ssh-user)
      SSH_USER="$2"; shift 2 ;;
    --repo-path)
      REPO_PATH="$2"; shift 2 ;;
    --branch)
      BRANCH="$2"; shift 2 ;;
    --frontend-www)
      FRONTEND_WWW="$2"; shift 2 ;;
    --server-service)
      SERVER_SERVICE="$2"; shift 2 ;;
    --backend-service)
      BACKEND_SERVICE="$2"; shift 2 ;;
    --nginx-service)
      NGINX_SERVICE="$2"; shift 2 ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 1 ;;
  esac
done

if [[ -z "$INSTANCE_ID" || -z "$HOST" || -z "$KEY_FILE" ]]; then
  echo "Error: --instance-id, --host and --key-file are required." >&2
  usage
  exit 1
fi

if [[ ! -f "$KEY_FILE" ]]; then
  echo "Error: key file not found: $KEY_FILE" >&2
  exit 1
fi

PUB_KEY_FILE="${KEY_FILE}.pub"
if [[ ! -f "$PUB_KEY_FILE" ]]; then
  echo "Error: public key file not found: $PUB_KEY_FILE" >&2
  echo "Tip: run 'ssh-keygen -y -f $KEY_FILE > $PUB_KEY_FILE'" >&2
  exit 1
fi

echo "==[1/5] Resolve AZ and push SSH public key via EC2 Instance Connect =="
AZ="$(aws ec2 describe-instances \
  --region "$REGION" \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' \
  --output text)"

if [[ -z "$AZ" || "$AZ" == "None" ]]; then
  echo "Error: could not determine availability zone for $INSTANCE_ID" >&2
  exit 1
fi

aws ec2-instance-connect send-ssh-public-key \
  --region "$REGION" \
  --instance-id "$INSTANCE_ID" \
  --availability-zone "$AZ" \
  --instance-os-user "$SSH_USER" \
  --ssh-public-key "file://${PUB_KEY_FILE}" >/dev/null

echo "==[2/5] Force fresh sync, cache clear, rebuild, restart on EC2 =="
ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" "$SSH_USER@$HOST" \
  "set -euo pipefail; \
   cd '$REPO_PATH'; \
   echo '--- host ---'; hostname; \
   echo '--- branch before ---'; git branch --show-current || true; \
   git fetch --prune origin '$BRANCH'; \
   git checkout '$BRANCH'; \
   git reset --hard 'origin/$BRANCH'; \
   git clean -fdx; \
   echo '--- commit deployed ---'; git --no-pager log --oneline -n 1; \
   rm -f server/cache/*.json || true; \
   find . -type d -name '__pycache__' -prune -exec rm -rf {} + || true; \
   find . -type d -name '.pytest_cache' -prune -exec rm -rf {} + || true; \
   rm -rf client/dist; \
   npm --prefix server ci; \
   npm --prefix client ci; \
   NODE_OPTIONS='--max-old-space-size=2048' npm --prefix client run build; \
   sudo rm -rf '$FRONTEND_WWW'/*; \
   sudo cp -r client/dist/* '$FRONTEND_WWW'/; \
   sudo rm -rf /var/cache/nginx/* || true; \
   sudo systemctl restart '$NGINX_SERVICE' '$SERVER_SERVICE' '$BACKEND_SERVICE'; \
   sleep 3; \
   echo '--- services ---'; \
   sudo systemctl is-active '$NGINX_SERVICE' '$SERVER_SERVICE' '$BACKEND_SERVICE'; \
   echo '--- static assets ---'; \
   ls -1 '$FRONTEND_WWW'/assets | sed -n '1,20p'; \
   echo '--- grep smoke ---'; \
   grep -R 'Read more' -n '$FRONTEND_WWW'/assets 2>/dev/null | head -n 5 || true; \
   grep -R 'dangerouslySetInnerHTML' -n '$FRONTEND_WWW'/assets 2>/dev/null | head -n 5 || true; \
   echo '--- health ---'; \
   curl -fsS 'http://127.0.0.1:5001/api/health'; echo; \
   curl -fsS 'http://127.0.0.1/api/health'; echo"

echo "==[3/5] Confirm deployed commit from remote =="
ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" "$SSH_USER@$HOST" \
  "cd '$REPO_PATH' && git rev-parse --short HEAD && git branch --show-current"

echo "==[4/5] Optional host diagnostics if build feels slow =="
ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" "$SSH_USER@$HOST" \
  "echo 'CPU:'; nproc; \
   echo 'MEM:'; free -h; \
   echo 'DISK:'; df -h / /tmp; \
   echo 'OOM tail:'; dmesg -T | grep -Ei 'killed process|out of memory|oom' | tail -n 10 || true"

echo "==[5/5] Done. If browser still stale, hard refresh once (Ctrl+Shift+R). =="

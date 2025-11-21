#!/usr/bin/env bash
set -euo pipefail

CONFIG_DIR=${DOCKER_GHCR_CONFIG:-/tmp/docker-ghcr}
PAT=${1:-}
USERNAME=${2:-${GHCR_USERNAME:-}}
if [[ -z "$PAT" || -z "$USERNAME" ]]; then
  echo "Usage: $0 <ghcr-pat> <github-username>" >&2
  exit 1
fi
mkdir -p "$CONFIG_DIR"
AUTH=$(printf '%s:%s' "$USERNAME" "$PAT" | base64)
cat > "$CONFIG_DIR/config.json" <<JSON
{
  "auths": {
    "ghcr.io": {
      "auth": "$AUTH"
    }
  }
}
JSON

echo "Wrote GHCR docker config to $CONFIG_DIR"
echo "Run: export DOCKER_CONFIG=$CONFIG_DIR"

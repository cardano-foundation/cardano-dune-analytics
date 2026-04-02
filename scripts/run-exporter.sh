#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
export PATH="$HOME/.local/bin:$PATH"
echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | yaci-s3 $* ==="
exec uv run python -m yaci_s3.cli "$@"

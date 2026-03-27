#!/usr/bin/env bash
set -euo pipefail
cd /home/kartik/cardano-dune-analytics
echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | yaci-s3 $* ==="
exec ~/.local/bin/uv run python -m yaci_s3.cli "$@"

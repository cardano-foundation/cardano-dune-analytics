#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
export PATH="$HOME/.local/bin:$PATH"

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Starting daily pipeline ==="

echo "--- Step 1/4: Dune exporters (13) ---"
uv run python -m yaci_s3.cli --dune --parallel 4

echo "--- Step 2/4: DRep profile update ---"
YESTERDAY=$(date -u -d "yesterday" +%Y-%m-%d)
uv run python -m yaci_s3.cli --internal drep_profile --date "$YESTERDAY"

echo "--- Step 3/4: Off-chain pool data (incremental) ---"
uv run python -m yaci_s3.cli --external off_chain_pool_data

echo "--- Step 4/4: DRep dist enriched ---"
uv run python -m yaci_s3.cli --hybrid drep_dist_enriched

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Daily pipeline complete ==="

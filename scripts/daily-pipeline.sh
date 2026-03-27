#!/usr/bin/env bash
set -euo pipefail
cd /home/kartik/cardano-dune-analytics

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Starting daily pipeline ==="

echo "--- Step 1/4: Dune exporters (12) ---"
~/.local/bin/uv run python -m yaci_s3.cli --dune --parallel 4

echo "--- Step 2/4: DRep profile update ---"
YESTERDAY=$(date -u -d "yesterday" +%Y-%m-%d)
~/.local/bin/uv run python -m yaci_s3.cli --internal drep_profile --date "$YESTERDAY"

echo "--- Step 3/4: Off-chain pool data (incremental) ---"
~/.local/bin/uv run python -m yaci_s3.cli --external off_chain_pool_data

echo "--- Step 4/4: DRep dist enriched ---"
~/.local/bin/uv run python -m yaci_s3.cli --hybrid drep_dist_enriched

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Daily pipeline complete ==="

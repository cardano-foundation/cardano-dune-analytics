#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
export PATH="$HOME/.local/bin:$PATH"

# Load .env for Telegram credentials
if [ -f .env ]; then
    set -a; source .env; set +a
fi

MAX_RETRIES=12
RETRY_INTERVAL=600  # 10 minutes
START_TIME=$(date -u +%s)

notify() {
    local message="$1"
    if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
        curl -s "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
          -d "chat_id=${TELEGRAM_CHAT_ID}" \
          -d "text=${message}" \
          -d "parse_mode=Markdown" > /dev/null 2>&1 || true
    fi
    if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
        curl -s -X POST "$SLACK_WEBHOOK_URL" \
          -H 'Content-type: application/json' \
          -d "{\"text\": \"${message}\"}" > /dev/null 2>&1 || true
    fi
}

duration() {
    local elapsed=$(( $(date -u +%s) - START_TIME ))
    printf '%dm %ds' $((elapsed / 60)) $((elapsed % 60))
}

CURRENT_STEP="starting"
trap 'notify "❌ *Daily pipeline FAILED* at step: $CURRENT_STEP ($(duration))
Check logs on UAT"' ERR

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Starting daily pipeline ==="

CURRENT_STEP="1/4 Dune exporters"
# --- Step 1: Poll for new dune partitions ---
DUNE_UPLOADED=0
DUNE_SUMMARY=""
for attempt in $(seq 1 $MAX_RETRIES); do
    echo "--- Step 1/4: Dune exporters (attempt $attempt/$MAX_RETRIES) ---"
    OUTPUT=$(uv run python -m yaci_s3.cli --dune --parallel 4 2>&1)
    echo "$OUTPUT"

    # Count uploaded partitions from summary lines
    UPLOADED=$(echo "$OUTPUT" | grep -oP 'uploaded=\K[0-9]+' | awk '{s+=$1} END {print s+0}')

    if [ "$UPLOADED" -gt 0 ]; then
        DUNE_UPLOADED=$UPLOADED
        DUNE_SUMMARY=$(echo "$OUTPUT" | grep -E 'new=[1-9]' | sed 's/.*\] */  /')
        echo "--- Dune: $UPLOADED partitions uploaded ---"
        break
    fi

    if [ "$attempt" -eq "$MAX_RETRIES" ]; then
        echo "--- Dune: no new partitions after $MAX_RETRIES attempts ---"
        break
    fi

    echo "--- No new partitions, retrying in 10 minutes... ---"
    sleep $RETRY_INTERVAL
done

CURRENT_STEP="2/4 DRep profile"
# --- Step 2: DRep profile update ---
echo "--- Step 2/4: DRep profile update ---"
YESTERDAY=$(date -u -d "yesterday" +%Y-%m-%d)
DREP_OUTPUT=$(uv run python -m yaci_s3.cli --internal drep_profile --date "$YESTERDAY" 2>&1)
echo "$DREP_OUTPUT"
DREP_PROFILES=$(echo "$DREP_OUTPUT" | grep -oP 'profiles_after\s+\K[0-9]+' || echo "n/a")

CURRENT_STEP="3/4 Off-chain pool data"
# --- Step 3: Off-chain pool data ---
echo "--- Step 3/4: Off-chain pool data (incremental) ---"
POOL_OUTPUT=$(uv run python -m yaci_s3.cli --external off_chain_pool_data 2>&1)
echo "$POOL_OUTPUT"
POOL_FETCHED=$(echo "$POOL_OUTPUT" | grep -oP 'fetched=\K[0-9]+' || echo "0")

CURRENT_STEP="4/4 DRep dist enriched"
# --- Step 4: DRep dist enriched ---
echo "--- Step 4/4: DRep dist enriched ---"
ENRICHED_OUTPUT=$(uv run python -m yaci_s3.cli --hybrid drep_dist_enriched 2>&1)
echo "$ENRICHED_OUTPUT"
ENRICHED_COUNT=$(echo "$ENRICHED_OUTPUT" | grep -oP 'uploaded=\K[0-9]+' || echo "0")

# --- Summary & notification ---
DURATION=$(duration)
echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Daily pipeline complete ($DURATION) ==="

MSG="*Daily pipeline complete* ($DURATION)
- Dune: $DUNE_UPLOADED partitions uploaded
- DRep profile: $DREP_PROFILES profiles
- Pool data: $POOL_FETCHED new/updated pools
- DRep enriched: $ENRICHED_COUNT new epochs"

notify "$MSG"

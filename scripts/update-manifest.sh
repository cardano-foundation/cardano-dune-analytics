#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
export PATH="$HOME/.local/bin:$PATH"

if [ -f .env ]; then
    set -a; source .env; set +a
fi

BUCKET="${S3_BUCKET:-cf-yaci-store-dataset}"
BASE_URL="https://d1gyygzinbyryv.cloudfront.net"
MANIFEST="/tmp/cloudfront_manifest.json"

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Updating CloudFront manifest ==="

# List all parquet files from S3
aws ${AWS_PROFILE:+--profile "$AWS_PROFILE"} s3 ls "s3://${BUCKET}/" --recursive \
    | grep '\.parquet$' \
    | awk '{print $4}' \
    | sort > /tmp/s3_parquet_files.txt

FILE_COUNT=$(wc -l < /tmp/s3_parquet_files.txt)
echo "Found $FILE_COUNT parquet files"

# Generate manifest JSON
uv run python -c "
import json

base_url = '${BASE_URL}'

with open('/tmp/s3_parquet_files.txt') as f:
    files = [line.strip() for line in f if line.strip()]

exporters = {}
for path in files:
    exporter = path.split('/')[0]
    if exporter not in exporters:
        exporters[exporter] = []
    exporters[exporter].append(path)

manifest = {
    'version': '1.0',
    'base_url': base_url,
    'total_files': len(files),
    'generated_at': '$(date -u +%Y-%m-%dT%H:%M:%SZ)',
    'exporters': {
        name: {
            'file_count': len(files_list),
            'files': sorted(files_list)
        }
        for name, files_list in sorted(exporters.items())
    }
}

with open('${MANIFEST}', 'w') as f:
    json.dump(manifest, f, indent=2)

print(f'Manifest: {len(files)} files, {len(exporters)} exporters')
"

# Upload manifest to S3
aws ${AWS_PROFILE:+--profile "$AWS_PROFILE"} s3 cp "$MANIFEST" "s3://${BUCKET}/cloudfront_manifest.json" \
    --content-type application/json --quiet

echo "Uploaded cloudfront_manifest.json to S3"

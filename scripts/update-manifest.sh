#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"
export PATH="$HOME/.local/bin:$PATH"

if [ -f .env ]; then
    set -a; source .env; set +a
fi

echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') | Updating CloudFront manifest ==="

uv run python -c "
import json, boto3, os
from datetime import datetime, timezone

bucket = os.getenv('S3_BUCKET', 'cf-yaci-store-dataset')
profile = os.getenv('AWS_PROFILE', '')
base_url = 'https://d1gyygzinbyryv.cloudfront.net'

session = boto3.Session(profile_name=profile) if profile else boto3.Session()
s3 = session.client('s3')

# List all parquet files
files = []
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket):
    for obj in page.get('Contents', []):
        if obj['Key'].endswith('.parquet'):
            files.append(obj['Key'])

files.sort()

# Group by exporter
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
    'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'exporters': {
        name: {
            'file_count': len(file_list),
            'files': sorted(file_list)
        }
        for name, file_list in sorted(exporters.items())
    }
}

manifest_json = json.dumps(manifest, indent=2)

# Upload to S3
s3.put_object(Bucket=bucket, Key='cloudfront_manifest.json',
              Body=manifest_json, ContentType='application/json')

print(f'Manifest: {len(files)} files, {len(exporters)} exporters')
print('Uploaded cloudfront_manifest.json to S3')
"

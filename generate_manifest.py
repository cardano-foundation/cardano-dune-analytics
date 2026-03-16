"""Generate manifest.json from S3 bucket contents and upload it."""

import json
import re
import sys
from datetime import datetime, timezone

import boto3


def generate_manifest(bucket: str, profile: str = "dune-test"):
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")

    print(f"Listing objects in s3://{bucket}/ ...")
    paginator = s3.get_paginator("list_objects_v2")

    exporters = {}
    total_size = 0
    total_files = 0

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            last_modified = obj["LastModified"].isoformat()

            # Skip the manifest itself
            if key in ("manifest.json", "index.html"):
                continue

            # Parse key: {exporter}/{YYYY-MM-DD}/{filename} or {exporter}/{N}/{filename}
            parts = key.split("/")
            if len(parts) != 3:
                continue

            exporter = parts[0]
            partition_value = parts[1]
            filename = parts[2]

            # Detect partition type from value format
            if re.match(r"^\d{4}-\d{2}-\d{2}$", partition_value):
                partition_type = "daily"
            elif re.match(r"^\d+$", partition_value):
                partition_type = "epoch"
            else:
                continue

            if exporter not in exporters:
                exporters[exporter] = {
                    "partition_type": partition_type,
                    "total_files": 0,
                    "total_size_bytes": 0,
                    "partitions": [],
                }

            exporters[exporter]["total_files"] += 1
            exporters[exporter]["total_size_bytes"] += size
            exporters[exporter]["partitions"].append({
                "partition_value": partition_value,
                "s3_key": key,
                "file_name": filename,
                "file_size_bytes": size,
                "last_modified": last_modified,
                "cloudfront_url": f"https://d2n685w72kr5g9.cloudfront.net/{key}",
            })

            total_files += 1
            total_size += size

    # Sort partitions within each exporter
    for exp_data in exporters.values():
        exp_data["partitions"].sort(key=lambda p: p["partition_value"])
        parts = exp_data["partitions"]
        if parts:
            exp_data["earliest_partition"] = parts[0]["partition_value"]
            exp_data["latest_partition"] = parts[-1]["partition_value"]

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bucket": bucket,
        "cloudfront_base_url": "https://d2n685w72kr5g9.cloudfront.net",
        "total_files": total_files,
        "total_size_bytes": total_size,
        "total_size_gb": round(total_size / (1024**3), 2),
        "exporters": dict(sorted(exporters.items())),
    }

    manifest_json = json.dumps(manifest, indent=2)
    print(f"Generated manifest: {total_files} files, {manifest['total_size_gb']} GB across {len(exporters)} exporters")

    # Upload manifest to S3
    print(f"Uploading manifest.json to s3://{bucket}/manifest.json ...")
    s3.put_object(
        Bucket=bucket,
        Key="manifest.json",
        Body=manifest_json.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Done. Accessible at: https://d2n685w72kr5g9.cloudfront.net/manifest.json")

    # Also save locally
    with open("manifest.json", "w") as f:
        f.write(manifest_json)
    print("Saved local copy: manifest.json")


if __name__ == "__main__":
    bucket = sys.argv[1] if len(sys.argv) > 1 else "cf-yaci-store-dataset"
    profile = sys.argv[2] if len(sys.argv) > 2 else "dune-test"
    generate_manifest(bucket, profile)

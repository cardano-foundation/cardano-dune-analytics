"""Migrate S3 keys from Hive-style (date=, epoch=) to bare paths.

Renames:
  block/date=2023-05-23/block-2023-05-23.parquet -> block/2023-05-23/block-2023-05-23.parquet
  reward/epoch=500/reward-epoch-500.parquet       -> reward/500/reward-epoch-500.parquet

Also updates SQLite tracking DB s3_key column.
"""

import re
import sqlite3
import sys

import boto3


def migrate(bucket: str, profile: str = "dune-test", db_path: str = "uploads.db", dry_run: bool = False):
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")

    # List all objects
    print(f"Listing objects in s3://{bucket}/ ...")
    paginator = s3.get_paginator("list_objects_v2")
    to_migrate = []

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            new_key = None

            # Match date= pattern
            m = re.match(r"^([^/]+)/date=(\d{4}-\d{2}-\d{2})/(.+)$", key)
            if m:
                new_key = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"

            # Match epoch= pattern
            if not new_key:
                m = re.match(r"^([^/]+)/epoch=(\d+)/(.+)$", key)
                if m:
                    new_key = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"

            if new_key and new_key != key:
                to_migrate.append((key, new_key))

    print(f"Found {len(to_migrate)} objects to migrate")

    if not to_migrate:
        print("Nothing to migrate.")
        return

    # Show first 5 examples
    for old, new in to_migrate[:5]:
        print(f"  {old} -> {new}")
    if len(to_migrate) > 5:
        print(f"  ... and {len(to_migrate) - 5} more")

    if dry_run:
        print("[DRY RUN] No changes made.")
        return

    # Migrate S3 objects: copy to new key, then delete old key
    success = 0
    errors = 0
    for i, (old_key, new_key) in enumerate(to_migrate, 1):
        try:
            # Copy
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": old_key},
                Key=new_key,
            )
            # Delete old
            s3.delete_object(Bucket=bucket, Key=old_key)
            success += 1
            if i % 100 == 0 or i == len(to_migrate):
                print(f"  Migrated {i}/{len(to_migrate)} objects...")
        except Exception as e:
            print(f"  ERROR migrating {old_key}: {e}")
            errors += 1

    print(f"S3 migration complete: {success} migrated, {errors} errors")

    # Update SQLite
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT id, s3_key FROM uploads")
        rows = cursor.fetchall()
        updated = 0
        for row_id, s3_key in rows:
            new_key = re.sub(r"/date=(\d{4}-\d{2}-\d{2})/", r"/\1/", s3_key)
            new_key = re.sub(r"/epoch=(\d+)/", r"/\1/", new_key)
            if new_key != s3_key:
                conn.execute("UPDATE uploads SET s3_key = ? WHERE id = ?", (new_key, row_id))
                updated += 1
        conn.commit()
        conn.close()
        print(f"SQLite updated: {updated} records")
    except Exception as e:
        print(f"SQLite update failed: {e}")


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    bucket = "cf-yaci-store-dataset"
    profile = "dune-test"
    db_path = "uploads.db"

    for arg in sys.argv[1:]:
        if arg.startswith("--bucket="):
            bucket = arg.split("=", 1)[1]
        elif arg.startswith("--profile="):
            profile = arg.split("=", 1)[1]
        elif arg.startswith("--db="):
            db_path = arg.split("=", 1)[1]

    migrate(bucket, profile, db_path, dry_run=dry)

"""S3 upload with retry and verification."""

import logging
import re
import time
from typing import List, Optional

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from .models import PartitionInfo

logger = logging.getLogger("yaci_s3.uploader")

UPLOAD_MAX_RETRIES = 10
UPLOAD_BASE_DELAY = 10  # seconds, exponential backoff: 10, 20, 40, 80, 120, 120, ...
UPLOAD_MAX_DELAY = 120  # cap backoff at 2 minutes
MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100 MB


def _build_s3_key(partition: PartitionInfo) -> str:
    """Build the S3 key for a partition."""
    if partition.partition_type == "daily":
        return (
            f"{partition.exporter}/{partition.partition_value}/"
            f"{partition.exporter}-{partition.partition_value}.parquet"
        )
    else:
        return (
            f"{partition.exporter}/{partition.partition_value}/"
            f"{partition.exporter}-epoch-{partition.partition_value}.parquet"
        )


class S3Uploader:
    """Handles S3 uploads with retry and verification."""

    def __init__(self, bucket: str, aws_profile: str = ""):
        self.bucket = bucket
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
        else:
            session = boto3.Session()
        self.s3 = session.client(
            "s3",
            config=BotoConfig(
                retries={"max_attempts": 0},
                connect_timeout=10,
                read_timeout=120,
            ),
        )
        self.transfer_config = TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
        )

    def upload(self, partition: PartitionInfo, dry_run: bool = False,
               s3_key_override: Optional[str] = None) -> Optional[str]:
        """Upload a parquet file to S3. Returns the S3 key on success, None on failure."""
        s3_key = s3_key_override or _build_s3_key(partition)

        if dry_run:
            logger.info("[DRY RUN] Would upload %s -> s3://%s/%s", partition.file_path, self.bucket, s3_key)
            return s3_key

        last_err = None
        for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
            try:
                logger.info(
                    "Uploading %s -> s3://%s/%s (attempt %d/%d)",
                    partition.file_path, self.bucket, s3_key, attempt, UPLOAD_MAX_RETRIES,
                )
                self.s3.upload_file(
                    partition.file_path, self.bucket, s3_key,
                    Config=self.transfer_config,
                )

                # Verify with HEAD
                if self._verify_upload(s3_key, partition.file_size):
                    logger.info("Upload verified: s3://%s/%s", self.bucket, s3_key)
                    return s3_key
                else:
                    logger.warning("Upload verification failed for %s", s3_key)
                    last_err = Exception("Upload verification failed: size mismatch")

            except (ClientError, Exception) as e:
                last_err = e
                logger.warning("Upload attempt %d/%d failed: %s", attempt, UPLOAD_MAX_RETRIES, e)

            if attempt < UPLOAD_MAX_RETRIES:
                delay = min(UPLOAD_BASE_DELAY * (2 ** (attempt - 1)), UPLOAD_MAX_DELAY)
                logger.info("Waiting %ds before retry...", delay)
                time.sleep(delay)

        logger.error("Upload failed after %d attempts: %s -> %s", UPLOAD_MAX_RETRIES, partition.file_path, s3_key)
        return None

    def _verify_upload(self, s3_key: str, expected_size: int) -> bool:
        """Verify upload via HEAD request + file size check."""
        try:
            resp = self.s3.head_object(Bucket=self.bucket, Key=s3_key)
            actual_size = resp["ContentLength"]
            if actual_size != expected_size:
                logger.warning(
                    "Size mismatch for %s: expected=%d, actual=%d",
                    s3_key, expected_size, actual_size,
                )
                return False
            return True
        except ClientError as e:
            logger.warning("HEAD check failed for %s: %s", s3_key, e)
            return False

    def list_all_objects(self, prefix: str = "") -> List[dict]:
        """List all objects in the bucket, returning parsed metadata for rebuild.

        Returns list of dicts with: s3_key, exporter, partition_value, file_name, file_size
        """
        objects = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                parsed = self._parse_s3_key(obj["Key"])
                if parsed:
                    parsed["file_size"] = obj["Size"]
                    objects.append(parsed)
        logger.info("Listed %d objects from s3://%s/%s", len(objects), self.bucket, prefix)
        return objects

    @staticmethod
    def _parse_s3_key(key: str) -> Optional[dict]:
        """Parse an S3 key into exporter/partition metadata.

        Expected patterns:
          {exporter}/{YYYY-MM-DD}/{filename}.parquet
          {exporter}/{N}/{filename}.parquet
        """
        # Daily pattern: exporter/YYYY-MM-DD/filename.parquet
        m = re.match(r"^([^/]+)/(\d{4}-\d{2}-\d{2})/(.+\.parquet)$", key)
        if m:
            return {
                "exporter": m.group(1),
                "partition_value": m.group(2),
                "s3_key": key,
                "file_name": m.group(3),
            }

        # Epoch pattern: exporter/N/filename.parquet
        m = re.match(r"^([^/]+)/(\d+)/(.+\.parquet)$", key)
        if m:
            return {
                "exporter": m.group(1),
                "partition_value": m.group(2),
                "s3_key": key,
                "file_name": m.group(3),
            }

        return None

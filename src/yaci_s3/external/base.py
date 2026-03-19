"""Base class for external data exporters."""

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import AppConfig
from ..db import TrackingDB
from ..models import PartitionInfo, UploadRecord
from ..uploader import S3Uploader

logger = logging.getLogger("yaci_s3.external.base")


class ExternalExporter(ABC):
    """Abstract base class for external API-based exporters."""

    # Subclasses must set this
    name: str = ""
    # Set True for exporters that produce multiple snapshots per day (e.g. asset_data)
    multi_snapshot: bool = False

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        self.config = config
        self.db = db
        self.uploader = uploader

    @abstractmethod
    def fetch_data(self) -> Optional[pa.Table]:
        """Fetch data from the external API.

        Returns a PyArrow Table, or None if no data to export.
        """

    @abstractmethod
    def validate(self, table: pa.Table) -> bool:
        """Validate the fetched data. Returns True if valid."""

    def run(self, dry_run: bool = False) -> dict:
        """Execute the full fetch -> write -> upload cycle."""
        run_id = self.db.start_external_run(self.name)
        summary = {
            "exporter": self.name,
            "records_fetched": 0,
            "records_exported": 0,
            "status": "completed",
        }

        try:
            # Fetch
            logger.info("[%s] Fetching data...", self.name)
            table = self.fetch_data()

            if table is None or len(table) == 0:
                logger.info("[%s] No data to export", self.name)
                self.db.complete_external_run(run_id, 0, 0, "completed")
                return summary

            summary["records_fetched"] = len(table)
            logger.info("[%s] Fetched %d records", self.name, len(table))

            # Validate
            if not self.validate(table):
                raise ValueError("Data validation failed")

            # Write parquet
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if self.multi_snapshot:
                suffix = self.db.get_next_suffix(self.name, date_str)
                partition_value = f"{date_str}.{suffix}"
            else:
                suffix = None
                partition_value = date_str

            local_path = self._write_parquet(table, date_str, suffix)
            file_size = os.path.getsize(local_path)
            summary["records_exported"] = len(table)

            logger.info(
                "[%s] Wrote %d rows to %s (%.1f MB)",
                self.name, len(table), local_path, file_size / (1024 * 1024),
            )

            if dry_run:
                logger.info("[%s] Dry run - skipping S3 upload", self.name)
                self.db.complete_external_run(run_id, len(table), len(table), "completed")
                return summary

            # Upload
            s3_key = self._build_s3_key(date_str, suffix)
            partition = PartitionInfo(
                exporter=self.name,
                partition_value=partition_value,
                partition_type="daily",
                file_path=local_path,
                file_size=file_size,
            )
            # Use the uploader directly with a custom s3_key
            uploaded_key = self._upload_to_s3(partition, s3_key, dry_run=False)

            if uploaded_key is None:
                raise RuntimeError(f"S3 upload failed for {s3_key}")

            # Track
            self.db.record_upload(UploadRecord(
                exporter=self.name,
                partition_value=partition_value,
                s3_key=uploaded_key,
                file_name=Path(local_path).name,
                row_count=len(table),
                min_slot=None,
                max_slot=None,
                file_size=file_size,
            ))

            logger.info("[%s] Upload complete: %s", self.name, uploaded_key)
            self.db.complete_external_run(run_id, len(table), len(table), "completed")

        except Exception as e:
            logger.error("[%s] Run failed: %s", self.name, e)
            summary["status"] = "failed"
            summary["error"] = str(e)
            self.db.complete_external_run(
                run_id, summary["records_fetched"], summary["records_exported"],
                "failed", str(e),
            )

        return summary

    def _write_parquet(self, table: pa.Table, date_str: str, suffix: Optional[int] = None) -> str:
        """Write a PyArrow Table to a local parquet file."""
        dir_path = os.path.join(
            self.config.base_data_path, self.name, f"date={date_str}",
        )
        os.makedirs(dir_path, exist_ok=True)
        if suffix is not None:
            file_name = f"{self.name}-{date_str}.{suffix}.parquet"
        else:
            file_name = f"{self.name}-{date_str}.parquet"
        file_path = os.path.join(dir_path, file_name)
        pq.write_table(table, file_path)
        return file_path

    def _build_s3_key(self, date_str: str, suffix: Optional[int] = None) -> str:
        """Build the S3 key for an external exporter file."""
        if suffix is not None:
            return f"{self.name}/{date_str}/{self.name}-{date_str}.{suffix}.parquet"
        return f"{self.name}/{date_str}/{self.name}-{date_str}.parquet"

    def _upload_to_s3(self, partition: PartitionInfo, s3_key: str,
                      dry_run: bool = False) -> Optional[str]:
        """Upload a file to S3 using the shared uploader, with a custom S3 key."""
        if dry_run:
            logger.info(
                "[DRY RUN] Would upload %s -> s3://%s/%s",
                partition.file_path, self.uploader.bucket, s3_key,
            )
            return s3_key

        from ..uploader import UPLOAD_MAX_RETRIES, UPLOAD_BASE_DELAY, UPLOAD_MAX_DELAY
        import time

        last_err = None
        for attempt in range(1, UPLOAD_MAX_RETRIES + 1):
            try:
                logger.info(
                    "Uploading %s -> s3://%s/%s (attempt %d/%d)",
                    partition.file_path, self.uploader.bucket, s3_key,
                    attempt, UPLOAD_MAX_RETRIES,
                )
                self.uploader.s3.upload_file(
                    partition.file_path, self.uploader.bucket, s3_key,
                    Config=self.uploader.transfer_config,
                )
                if self.uploader._verify_upload(s3_key, partition.file_size):
                    return s3_key
                else:
                    last_err = Exception("Upload verification failed: size mismatch")
            except Exception as e:
                last_err = e
                logger.warning("Upload attempt %d/%d failed: %s", attempt, UPLOAD_MAX_RETRIES, e)

            if attempt < UPLOAD_MAX_RETRIES:
                delay = min(UPLOAD_BASE_DELAY * (2 ** (attempt - 1)), UPLOAD_MAX_DELAY)
                logger.info("Waiting %ds before retry...", delay)
                time.sleep(delay)

        logger.error("Upload failed after %d attempts: %s", UPLOAD_MAX_RETRIES, s3_key)
        return None

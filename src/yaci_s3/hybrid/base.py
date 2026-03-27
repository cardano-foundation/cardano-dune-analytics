"""Base class for hybrid exporters that join local data sources."""

import logging
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import AppConfig
from ..db import TrackingDB
from ..models import ExporterDef, PartitionInfo, UploadRecord
from ..scanner import scan_exporter
from ..uploader import S3Uploader

logger = logging.getLogger("yaci_s3.hybrid.base")


class HybridExporter(ABC):
    """Abstract base class for hybrid exporters.

    Hybrid exporters scan a source exporter's partitions, enrich the data
    by joining with other local sources, and upload the result to S3.
    """

    name: str = ""
    source_exporter_name: str = ""
    source_partition_type: str = "epoch"

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        self.config = config
        self.db = db
        self.uploader = uploader

    @abstractmethod
    def enrich(self, source_table: pa.Table, partition_value: str) -> pa.Table:
        """Enrich the source data. Returns the enriched table."""

    def _source_exporter_def(self) -> ExporterDef:
        """Create a synthetic ExporterDef for scanning source partitions."""
        return ExporterDef(
            name=self.source_exporter_name,
            pg_table="",
            slot_column="",
            partition_type=self.source_partition_type,
        )

    def _build_s3_key(self, partition_value: str) -> str:
        if self.source_partition_type == "epoch":
            return f"{self.name}/{partition_value}/{self.name}-epoch-{partition_value}.parquet"
        return f"{self.name}/{partition_value}/{self.name}-{partition_value}.parquet"

    def run(self, dry_run: bool = False, partition_filter: Optional[set] = None) -> dict:
        """Scan source partitions, enrich, write, upload."""
        run_id = self.db.start_external_run(self.name)
        summary = {
            "exporter": self.name,
            "scanned": 0,
            "skipped_existing": 0,
            "enriched": 0,
            "uploaded": 0,
            "errors": 0,
            "status": "completed",
        }

        try:
            # Scan source partitions
            source_def = self._source_exporter_def()
            partitions = scan_exporter(self.config.base_data_path, source_def)
            summary["scanned"] = len(partitions)

            if partition_filter:
                partitions = [p for p in partitions if p.partition_value in partition_filter]

            # Filter already uploaded (under our name, not source name)
            uploaded_set = self.db.get_uploaded_partitions(self.name)
            new_partitions = [p for p in partitions if p.partition_value not in uploaded_set]
            summary["skipped_existing"] = len(partitions) - len(new_partitions)

            logger.info(
                "[%s] %d source partitions, %d new, %d already uploaded",
                self.name, len(partitions), len(new_partitions), summary["skipped_existing"],
            )

            if not new_partitions:
                self.db.complete_external_run(run_id, 0, 0, "completed")
                return summary

            # Compute max partition value as source watermark
            max_partition = max(p.partition_value for p in new_partitions)

            for partition in new_partitions:
                try:
                    # Read source (use ParquetFile to avoid dataset partition inference)
                    source_table = pq.ParquetFile(partition.file_path).read()

                    # Enrich
                    enriched = self.enrich(source_table, partition.partition_value)
                    summary["enriched"] += 1

                    # Write locally (atomic via temp file)
                    out_dir = Path(self.config.export_data_path) / self.name / f"epoch={partition.partition_value}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f"{self.name}-epoch-{partition.partition_value}.parquet"
                    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=str(out_dir))
                    os.close(fd)
                    try:
                        pq.write_table(enriched, tmp_path)
                        os.replace(tmp_path, str(out_file))
                    except Exception:
                        if os.path.exists(tmp_path):
                            os.unlink(tmp_path)
                        raise
                    file_size = os.path.getsize(str(out_file))

                    logger.info(
                        "[%s] Enriched partition %s: %d rows (%.1f MB)",
                        self.name, partition.partition_value, len(enriched),
                        file_size / (1024 * 1024),
                    )

                    if dry_run:
                        logger.info("[%s] Dry run - skipping upload for %s", self.name, partition.partition_value)
                        continue

                    # Upload
                    s3_key = self._build_s3_key(partition.partition_value)
                    enriched_partition = PartitionInfo(
                        exporter=self.name,
                        partition_value=partition.partition_value,
                        partition_type=self.source_partition_type,
                        file_path=str(out_file),
                        file_size=file_size,
                    )
                    uploaded_key = self.uploader.upload(enriched_partition, dry_run=False, s3_key_override=s3_key)

                    if uploaded_key is None:
                        logger.error("[%s] Upload failed for %s", self.name, partition.partition_value)
                        summary["errors"] += 1
                        continue

                    # Track
                    self.db.record_upload(UploadRecord(
                        exporter=self.name,
                        partition_value=partition.partition_value,
                        s3_key=uploaded_key,
                        file_name=out_file.name,
                        row_count=len(enriched),
                        min_slot=None,
                        max_slot=None,
                        file_size=file_size,
                    ))
                    summary["uploaded"] += 1
                    logger.info("[%s] Uploaded %s -> %s", self.name, partition.partition_value, uploaded_key)

                except Exception as e:
                    logger.error("[%s] Error processing partition %s: %s",
                                 self.name, partition.partition_value, e)
                    summary["errors"] += 1

            self.db.complete_external_run(
                run_id, summary["enriched"], summary["uploaded"],
                "completed", source_data_watermark=max_partition,
            )

        except Exception as e:
            logger.error("[%s] Run failed: %s", self.name, e)
            summary["status"] = "failed"
            summary["error"] = str(e)
            self.db.complete_external_run(
                run_id, summary.get("enriched", 0), summary.get("uploaded", 0),
                "failed", str(e),
            )

        return summary

"""Filesystem scanner for discovering local parquet partitions."""

import logging
import os
import re
from pathlib import Path
from typing import List

from .models import ExporterDef, PartitionInfo

logger = logging.getLogger("yaci_s3.scanner")

DATE_RE = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")
EPOCH_RE = re.compile(r"^epoch=(\d+)$")


def scan_exporter(base_path: str, exporter: ExporterDef) -> List[PartitionInfo]:
    """Discover all partitions for a given exporter under base_path/{exporter.name}/."""
    exporter_dir = Path(base_path) / exporter.name
    if not exporter_dir.is_dir():
        logger.warning("Exporter directory not found: %s", exporter_dir)
        return []

    partitions = []
    for entry in sorted(exporter_dir.iterdir()):
        if not entry.is_dir():
            continue

        if exporter.partition_type == "daily":
            m = DATE_RE.match(entry.name)
            if not m:
                continue
            partition_value = m.group(1)
            expected_file = f"{exporter.name}-{partition_value}.parquet"
        elif exporter.partition_type == "epoch":
            m = EPOCH_RE.match(entry.name)
            if not m:
                continue
            partition_value = m.group(1)
            expected_file = f"{exporter.name}-epoch-{partition_value}.parquet"
        else:
            continue

        parquet_path = entry / expected_file
        if not parquet_path.is_file():
            # Try finding any .parquet file in the directory
            parquet_files = list(entry.glob("*.parquet"))
            if parquet_files:
                parquet_path = parquet_files[0]
            else:
                logger.debug("No parquet file in %s", entry)
                continue

        partitions.append(PartitionInfo(
            exporter=exporter.name,
            partition_value=partition_value,
            partition_type=exporter.partition_type,
            file_path=str(parquet_path),
            file_size=parquet_path.stat().st_size,
        ))

    logger.info("Found %d partitions for exporter '%s'", len(partitions), exporter.name)
    return partitions

"""Off-chain pool data exporter - fetches pool metadata from Blockfrost."""

import logging
from datetime import datetime, timezone
from glob import glob as pyglob
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa

from . import register
from .base import ExternalExporter
from ..config import AppConfig
from ..db import TrackingDB
from ..internal.anchor_resolver import resolve_pool_batch
from ..uploader import S3Uploader

logger = logging.getLogger("yaci_s3.external.off_chain_pool_data")


def _read_pool_hashes(base_data_path: str) -> list:
    """Read pool parquet files and return list of distinct pool hashes."""
    pool_dir = Path(base_data_path) / "pool"

    if not pool_dir.is_dir():
        logger.warning("pool directory not found: %s", pool_dir)
        return []

    files_list = sorted(pyglob(f"{base_data_path}/pool/date=*/*.parquet"))
    if not files_list:
        logger.warning("No pool parquet files found")
        return []

    logger.info("Reading %d pool file(s)", len(files_list))

    conn = duckdb.connect(":memory:")
    try:
        result = conn.execute(
            "SELECT DISTINCT pool_id FROM read_parquet(?) ORDER BY pool_id",
            [files_list],
        ).fetchall()
    except Exception as e:
        logger.error("DuckDB read failed: %s", e)
        return []
    finally:
        conn.close()

    pool_hashes = [row[0] for row in result]
    logger.info("Read %d unique pools", len(pool_hashes))
    return pool_hashes


@register("off_chain_pool_data")
class OffChainPoolDataExporter(ExternalExporter):
    """Exports pool off-chain metadata from Blockfrost."""

    name = "off_chain_pool_data"

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        super().__init__(config, db, uploader)

    def fetch_data(self) -> Optional[pa.Table]:
        """Read on-chain pool hashes and resolve metadata via Blockfrost."""
        pool_hashes = _read_pool_hashes(self.config.base_data_path)
        if not pool_hashes:
            return None

        logger.info("Resolving metadata for %d pools", len(pool_hashes))
        results = resolve_pool_batch(
            pool_hashes,
            project_id=self.config.blockfrost_project_id,
            max_workers=self.config.anchor_max_workers,
        )

        now = datetime.now(timezone.utc).isoformat()

        rows = []
        success_count = 0
        fail_count = 0
        for pool_hash, meta in results.items():
            if meta.success:
                rows.append({
                    "pool_hash": pool_hash,
                    "pool_id": meta.pool_id,
                    "ticker": meta.ticker,
                    "name": meta.name,
                    "description": meta.description,
                    "homepage": meta.homepage,
                    "metadata_url": meta.metadata_url,
                    "metadata_hash": meta.metadata_hash,
                    "fetched_at": now,
                })
                success_count += 1
            else:
                fail_count += 1

        logger.info("Resolution: %d success, %d failed", success_count, fail_count)

        if not rows:
            return None

        schema = pa.schema([
            ("pool_hash", pa.string()),
            ("pool_id", pa.string()),
            ("ticker", pa.string()),
            ("name", pa.string()),
            ("description", pa.string()),
            ("homepage", pa.string()),
            ("metadata_url", pa.string()),
            ("metadata_hash", pa.string()),
            ("fetched_at", pa.string()),
        ])

        arrays = {col.name: pa.array([r[col.name] for r in rows], type=col.type)
                  for col in schema}
        return pa.table(arrays, schema=schema)

    def validate(self, table: pa.Table) -> bool:
        """Validate pool data: non-empty, unique pool_hash."""
        if len(table) == 0:
            logger.error("Validation failed: empty table")
            return False

        hashes = table.column("pool_hash").to_pylist()
        if len(set(hashes)) != len(hashes):
            logger.error("Validation failed: duplicate pool hashes")
            return False

        logger.info("Validation passed: %d unique pools", len(hashes))
        return True

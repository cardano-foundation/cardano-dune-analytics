"""Off-chain pool data exporter - fetches pool metadata from Blockfrost."""

import logging
from datetime import datetime, timezone
from glob import glob as pyglob
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from . import register
from .base import ExternalExporter
from ..config import AppConfig
from ..db import TrackingDB
from ..internal.anchor_resolver import resolve_pool_batch
from ..uploader import S3Uploader

logger = logging.getLogger("yaci_s3.external.off_chain_pool_data")

SCHEMA = pa.schema([
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


def _read_already_exported(base_data_path: str) -> set:
    """Read pool_hash values from existing off_chain_pool_data parquet exports."""
    export_dir = Path(base_data_path) / "off_chain_pool_data"
    if not export_dir.is_dir():
        return set()

    files_list = sorted(pyglob(f"{base_data_path}/off_chain_pool_data/date=*/*.parquet"))
    if not files_list:
        return set()

    already = set()
    for f in files_list:
        try:
            table = pq.read_table(f, columns=["pool_hash"])
            already.update(table.column("pool_hash").to_pylist())
        except Exception as e:
            logger.warning("Failed to read %s: %s", f, e)

    logger.info("Found %d already-exported pool hashes", len(already))
    return already


@register("off_chain_pool_data")
class OffChainPoolDataExporter(ExternalExporter):
    """Exports pool off-chain metadata from Blockfrost.

    Supports two modes:
    - rebuild: resolve ALL unique pools from on-chain data
    - incremental (default): only resolve pools not already in previous exports
    """

    name = "off_chain_pool_data"

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        super().__init__(config, db, uploader)
        self._rebuild = False

    def fetch_data(self) -> Optional[pa.Table]:
        """Read on-chain pool hashes and resolve metadata via Blockfrost.

        In incremental mode, skips pools already present in previous exports.
        """
        all_pool_hashes = _read_pool_hashes(self.config.base_data_path)
        if not all_pool_hashes:
            return None

        if self._rebuild:
            pools_to_resolve = all_pool_hashes
            logger.info("[REBUILD] Resolving all %d pools", len(pools_to_resolve))
        else:
            already_exported = _read_already_exported(self.config.base_data_path)
            pools_to_resolve = [h for h in all_pool_hashes if h not in already_exported]
            logger.info(
                "Incremental: %d total pools, %d already exported, %d to resolve",
                len(all_pool_hashes), len(already_exported), len(pools_to_resolve),
            )

        if not pools_to_resolve:
            logger.info("No new pools to resolve")
            return None

        results = resolve_pool_batch(
            pools_to_resolve,
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

        arrays = {col.name: pa.array([r[col.name] for r in rows], type=col.type)
                  for col in SCHEMA}
        return pa.table(arrays, schema=SCHEMA)

    def run(self, dry_run: bool = False, rebuild: bool = False) -> dict:
        """Run the exporter. Set rebuild=True to re-resolve all pools."""
        self._rebuild = rebuild
        return super().run(dry_run=dry_run)

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

"""Off-chain pool data exporter - fetches pool metadata from Blockfrost."""

import logging
import re
from datetime import datetime, timezone
from glob import glob as pyglob
from pathlib import Path
from typing import Optional, Set

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
    ("fetched_at", pa.timestamp('us', tz='UTC')),
    ("version", pa.int32()),
])

DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")


def _read_all_pool_hashes(base_data_path: str) -> list:
    """Read pool_registration parquet files and return list of distinct pool hashes."""
    pool_dir = Path(base_data_path) / "pool_registration"

    if not pool_dir.is_dir():
        logger.warning("pool directory not found: %s", pool_dir)
        return []

    files_list = sorted(pyglob(f"{base_data_path}/pool_registration/date=*/*.parquet"))
    if not files_list:
        logger.warning("No pool_registration parquet files found")
        return []

    logger.info("Reading %d pool_registration file(s)", len(files_list))

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


def _read_pool_hashes_since(base_data_path: str, since_date: str) -> set:
    """Read pool hashes from pool_registration partitions after since_date.

    Returns the set of pool hashes that appear in date partitions > since_date.
    These are pools that were newly registered or updated.
    """
    pool_dir = Path(base_data_path) / "pool_registration"
    if not pool_dir.is_dir():
        return set()

    all_files = sorted(pyglob(f"{base_data_path}/pool_registration/date=*/*.parquet"))
    # Filter to files with date > since_date
    new_files = []
    for f in all_files:
        m = DATE_RE.search(f)
        if m and m.group(1) > since_date:
            new_files.append(f)

    if not new_files:
        return set()

    logger.info("Reading %d new pool_registration file(s) since %s", len(new_files), since_date)

    conn = duckdb.connect(":memory:")
    try:
        result = conn.execute(
            "SELECT DISTINCT pool_id FROM read_parquet(?)",
            [new_files],
        ).fetchall()
    except Exception as e:
        logger.error("DuckDB read failed: %s", e)
        return set()
    finally:
        conn.close()

    hashes = {row[0] for row in result}
    logger.info("Found %d pools in new partitions", len(hashes))
    return hashes


def _get_last_export_date(base_data_path: str) -> Optional[str]:
    """Get the latest date from existing off_chain_pool_data exports."""
    export_dir = Path(base_data_path) / "off_chain_pool_data"
    if not export_dir.is_dir():
        return None

    dates = []
    for d in export_dir.iterdir():
        m = DATE_RE.search(d.name)
        if m:
            dates.append(m.group(1))

    return max(dates) if dates else None


def _read_already_exported(base_data_path: str) -> Set[str]:
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


def _read_max_versions(base_data_path: str) -> dict:
    """Read max version per pool_hash from existing exports.

    Returns {pool_hash: max_version}. Handles pre-migration files without
    a version column by treating all rows as version 1.
    """
    files_list = sorted(pyglob(f"{base_data_path}/off_chain_pool_data/date=*/*.parquet"))
    if not files_list:
        return {}

    versions = {}  # pool_hash -> max_version
    for f in files_list:
        try:
            schema = pq.read_schema(f)
            if "version" in schema.names:
                table = pq.read_table(f, columns=["pool_hash", "version"])
                for ph, v in zip(
                    table.column("pool_hash").to_pylist(),
                    table.column("version").to_pylist(),
                ):
                    versions[ph] = max(versions.get(ph, 0), v or 1)
            else:
                table = pq.read_table(f, columns=["pool_hash"])
                for ph in table.column("pool_hash").to_pylist():
                    versions[ph] = max(versions.get(ph, 0), 1)
        except Exception as e:
            logger.warning("Failed to read versions from %s: %s", f, e)

    return versions


@register("off_chain_pool_data")
class OffChainPoolDataExporter(ExternalExporter):
    """Exports pool off-chain metadata from Blockfrost.

    Supports two modes:
    - rebuild (--rebuild): resolve ALL unique pools from on-chain data
    - incremental (default): resolve only new/updated pools since last export
      - New pools: pool_hash not in any previous export
      - Updated pools: pool_hash appears in pool date partitions after last export date
    """

    name = "off_chain_pool_data"

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        super().__init__(config, db, uploader)
        self._rebuild = False

    def _compute_pool_reg_max_date(self) -> Optional[str]:
        """Get the max date partition from pool_registration data."""
        pool_dir = Path(self.config.base_data_path) / "pool_registration"
        if not pool_dir.is_dir():
            return None
        dates = []
        for d in pool_dir.iterdir():
            m = DATE_RE.search(d.name)
            if m:
                dates.append(m.group(1))
        return max(dates) if dates else None

    def get_source_watermark(self) -> Optional[str]:
        return self._source_watermark

    def fetch_data(self) -> Optional[pa.Table]:
        """Read on-chain pool hashes and resolve metadata via Blockfrost."""
        self._source_watermark = self._compute_pool_reg_max_date()
        if self._rebuild:
            pools_to_resolve = _read_all_pool_hashes(self.config.base_data_path)
            if not pools_to_resolve:
                return None
            logger.info("[REBUILD] Resolving all %d pools", len(pools_to_resolve))
        else:
            pools_to_resolve = self._find_incremental_pools()
            if not pools_to_resolve:
                logger.info("No new or updated pools to resolve")
                return None

        results = resolve_pool_batch(
            pools_to_resolve,
            project_id=self.config.blockfrost_project_id,
            max_workers=self.config.anchor_max_workers,
        )

        now = datetime.now(timezone.utc)

        if self._rebuild:
            max_versions = {}
        else:
            max_versions = _read_max_versions(self.config.base_data_path)

        rows = []
        success_count = 0
        fail_count = 0
        for pool_hash, meta in results.items():
            if meta.success:
                version = 1 if self._rebuild else max_versions.get(pool_hash, 0) + 1
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
                    "version": version,
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

    def _find_incremental_pools(self) -> list:
        """Find pools that need resolution: new pools + updated pools."""
        base = self.config.base_data_path

        # Get all pool hashes and previously exported ones
        all_hashes = set(_read_all_pool_hashes(base))
        if not all_hashes:
            return []

        already_exported = _read_already_exported(base)

        # New pools: not in any previous export
        new_pools = all_hashes - already_exported

        # Updated pools: appear in pool partitions after the source watermark
        updated_pools = set()
        last_export_date = self.db.get_last_source_watermark(self.name)
        if last_export_date:
            changed_pools = _read_pool_hashes_since(base, last_export_date)
            # Only count as updated if they were already exported (otherwise they're new)
            updated_pools = changed_pools & already_exported

        pools_to_resolve = sorted(new_pools | updated_pools)

        logger.info(
            "Incremental: %d total on-chain, %d already exported, "
            "%d new, %d updated, %d to resolve",
            len(all_hashes), len(already_exported),
            len(new_pools), len(updated_pools), len(pools_to_resolve),
        )

        return pools_to_resolve

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

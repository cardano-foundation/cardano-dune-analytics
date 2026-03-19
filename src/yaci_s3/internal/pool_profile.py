"""Pool profile builder: reads pool parquet files, resolves off-chain metadata via Blockfrost, writes profile."""

import logging
import os
import tempfile
from datetime import datetime, timezone
from glob import glob as pyglob
from pathlib import Path
from typing import List, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import AppConfig
from . import register
from .anchor_resolver import resolve_pool_batch

logger = logging.getLogger("yaci_s3.internal.pool_profile")

PROFILE_SCHEMA = pa.schema([
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


def _profile_dir(config: AppConfig) -> Path:
    return Path(config.base_data_path) / "pool_profile"


def _profile_path(config: AppConfig) -> Path:
    return _profile_dir(config) / "pool_profile.parquet"


def _load_existing_profile(config: AppConfig) -> dict:
    """Load existing profile parquet into a dict keyed by pool_hash."""
    path = _profile_path(config)
    if not path.exists():
        return {}

    table = pq.read_table(str(path))
    profiles = {}
    for i in range(len(table)):
        row = {col: table.column(col)[i].as_py() for col in table.column_names}
        profiles[row["pool_hash"]] = row
    return profiles


def _read_pools(config: AppConfig) -> list:
    """Read pool parquet files and return list of distinct pool hashes."""
    base = config.base_data_path
    pool_dir = Path(base) / "pool"

    if not pool_dir.is_dir():
        logger.warning("pool directory not found: %s", pool_dir)
        return []

    files_list = sorted(pyglob(f"{base}/pool/date=*/*.parquet"))
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


def _build_profiles(
    pool_hashes: list,
    existing: dict,
    config: AppConfig,
    dry_run: bool = False,
) -> dict:
    """Build/update profiles from pool data.

    Only successfully resolved pools are stored. Failed resolutions are skipped
    (existing successful entries are preserved).

    Returns updated profiles dict keyed by pool_hash.
    """
    if not pool_hashes:
        return existing

    now_iso = datetime.now(timezone.utc).isoformat()

    # Determine which pools need resolution (not already successfully resolved)
    pools_to_resolve = [h for h in pool_hashes if h not in existing]

    logger.info(
        "Profile update: %d pools to resolve, %d already resolved",
        len(pools_to_resolve),
        len(pool_hashes) - len(pools_to_resolve),
    )

    if dry_run:
        logger.info("[DRY RUN] Would resolve %d pools", len(pools_to_resolve))
        return existing

    profiles = dict(existing)

    if pools_to_resolve:
        results = resolve_pool_batch(
            pools_to_resolve,
            project_id=config.blockfrost_project_id,
            max_workers=config.anchor_max_workers,
        )

        success_count = 0
        fail_count = 0
        for pool_hash, meta_result in results.items():
            if meta_result.success:
                profiles[pool_hash] = {
                    "pool_hash": pool_hash,
                    "pool_id": meta_result.pool_id,
                    "ticker": meta_result.ticker,
                    "name": meta_result.name,
                    "description": meta_result.description,
                    "homepage": meta_result.homepage,
                    "metadata_url": meta_result.metadata_url,
                    "metadata_hash": meta_result.metadata_hash,
                    "fetched_at": now_iso,
                }
                success_count += 1
            else:
                fail_count += 1

        logger.info("Resolution: %d success, %d failed", success_count, fail_count)

    return profiles


def _write_profile(profiles: dict, config: AppConfig):
    """Write profile dict to parquet atomically."""
    if not profiles:
        logger.warning("No profiles to write")
        return

    out_dir = _profile_dir(config)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = _profile_path(config)

    # Build columns
    columns = {field.name: [] for field in PROFILE_SCHEMA}
    for pool_hash in sorted(profiles.keys()):
        row = profiles[pool_hash]
        for col in columns:
            columns[col].append(row.get(col))

    table = pa.table(columns, schema=PROFILE_SCHEMA)

    # Atomic write via temp file
    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=str(out_dir))
    os.close(fd)
    try:
        pq.write_table(table, tmp_path)
        os.replace(tmp_path, str(out_path))
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info("Wrote pool profile: %d rows -> %s", len(table), out_path)


@register("pool_profile")
class PoolProfileJob:
    """Internal job: build/update the pool off-chain metadata lookup table."""

    name = "pool_profile"

    def __init__(self, config: AppConfig):
        self.config = config

    def run(
        self,
        rebuild: bool = False,
        dates: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> dict:
        """Run the pool profile build/update.

        Args:
            rebuild: If True, rebuild from all pool files (ignores existing profile).
            dates: Unused (pools are always read in full). Kept for interface compatibility.
            dry_run: Show what would change without writing.

        Returns summary dict.
        """
        summary = {
            "job": self.name,
            "status": "completed",
            "pools_read": 0,
            "profiles_before": 0,
            "profiles_after": 0,
        }

        try:
            if rebuild:
                existing = {}
            else:
                existing = _load_existing_profile(self.config)
            summary["profiles_before"] = len(existing)

            pool_hashes = _read_pools(self.config)
            summary["pools_read"] = len(pool_hashes)

            if not pool_hashes:
                logger.info("No pools to process")
                summary["profiles_after"] = len(existing)
                return summary

            profiles = _build_profiles(pool_hashes, existing, self.config, dry_run=dry_run)
            summary["profiles_after"] = len(profiles)

            if not dry_run:
                _write_profile(profiles, self.config)

            logger.info(
                "Pool profile %s complete: %d -> %d profiles (%d pools read)",
                "rebuild" if rebuild else "update",
                summary["profiles_before"], summary["profiles_after"],
                summary["pools_read"],
            )

        except Exception as e:
            logger.error("Pool profile job failed: %s", e)
            summary["status"] = "failed"
            summary["error"] = str(e)

        return summary

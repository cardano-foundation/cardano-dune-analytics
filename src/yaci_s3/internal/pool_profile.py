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
    ("pool_id", pa.string()),
    ("ticker", pa.string()),
    ("name", pa.string()),
    ("description", pa.string()),
    ("homepage", pa.string()),
    ("metadata_url", pa.string()),
    ("metadata_hash", pa.string()),
    ("latest_status", pa.string()),
    ("latest_epoch", pa.int64()),
    ("latest_slot", pa.int64()),
    ("latest_tx_hash", pa.string()),
    ("latest_date", pa.string()),
    ("fetch_status", pa.string()),
    ("http_status", pa.int32()),
    ("last_checked_at", pa.string()),
    ("updated_at", pa.string()),
])


def _profile_dir(config: AppConfig) -> Path:
    return Path(config.base_data_path) / "pool_profile"


def _profile_path(config: AppConfig) -> Path:
    return _profile_dir(config) / "pool_profile.parquet"


def _load_existing_profile(config: AppConfig) -> dict:
    """Load existing profile parquet into a dict keyed by pool_id."""
    path = _profile_path(config)
    if not path.exists():
        return {}

    table = pq.read_table(str(path))
    profiles = {}
    for i in range(len(table)):
        row = {col: table.column(col)[i].as_py() for col in table.column_names}
        profiles[row["pool_id"]] = row
    return profiles


def _read_pools(config: AppConfig) -> pa.Table:
    """Read pool parquet files and return latest event per pool_id."""
    base = config.base_data_path
    pool_dir = Path(base) / "pool"

    if not pool_dir.is_dir():
        logger.warning("pool directory not found: %s", pool_dir)
        return pa.table({})

    files_list = sorted(pyglob(f"{base}/pool/date=*/*.parquet"))
    if not files_list:
        logger.warning("No pool parquet files found")
        return pa.table({})

    logger.info("Reading %d pool file(s)", len(files_list))

    conn = duckdb.connect(":memory:")
    try:
        result = conn.execute(
            """
            WITH ranked AS (
                SELECT pool_id, status, epoch, slot, tx_hash,
                       regexp_extract(filename, 'date=([^/]+)', 1) as source_date,
                       ROW_NUMBER() OVER (PARTITION BY pool_id ORDER BY slot DESC) as rn
                FROM read_parquet(?, filename=true)
            )
            SELECT pool_id, status, epoch, slot, tx_hash, source_date
            FROM ranked WHERE rn = 1
            ORDER BY pool_id
            """,
            [files_list],
        ).fetch_arrow_table()
    except Exception as e:
        logger.error("DuckDB read failed: %s", e)
        return pa.table({})
    finally:
        conn.close()

    logger.info("Read %d unique pools", len(result))
    return result


def _build_profiles(
    pools: pa.Table,
    existing: dict,
    config: AppConfig,
    dry_run: bool = False,
) -> dict:
    """Build/update profiles from pool data.

    Returns updated profiles dict.
    """
    if len(pools) == 0:
        return existing

    now_iso = datetime.now(timezone.utc).isoformat()

    # Collect pool info
    pool_info = {}
    for i in range(len(pools)):
        pool_id = pools.column("pool_id")[i].as_py()
        pool_info[pool_id] = {
            "pool_id": pool_id,
            "status": pools.column("status")[i].as_py(),
            "epoch": pools.column("epoch")[i].as_py(),
            "slot": pools.column("slot")[i].as_py(),
            "tx_hash": pools.column("tx_hash")[i].as_py(),
            "source_date": pools.column("source_date")[i].as_py(),
        }

    # Determine which pools need resolution
    pools_to_resolve = []
    for pool_id, info in pool_info.items():
        ex = existing.get(pool_id)
        # Skip if existing profile has same or newer slot
        if ex and ex.get("latest_slot") is not None and info["slot"] <= ex["latest_slot"]:
            continue
        pools_to_resolve.append(pool_id)

    logger.info(
        "Profile update: %d pools to resolve, %d unchanged",
        len(pools_to_resolve),
        len(pool_info) - len(pools_to_resolve),
    )

    if dry_run:
        logger.info("[DRY RUN] Would resolve %d pools", len(pools_to_resolve))
        return existing

    # Resolve metadata via Blockfrost in parallel
    profiles = dict(existing)

    if pools_to_resolve:
        results = resolve_pool_batch(
            pools_to_resolve,
            project_id=config.blockfrost_project_id,
            max_workers=config.anchor_max_workers,
        )

        success_count = 0
        fail_count = 0
        for pool_id, meta_result in results.items():
            info = pool_info[pool_id]

            if meta_result.success:
                profiles[pool_id] = {
                    "pool_id": pool_id,
                    "ticker": meta_result.ticker,
                    "name": meta_result.name,
                    "description": meta_result.description,
                    "homepage": meta_result.homepage,
                    "metadata_url": meta_result.metadata_url,
                    "metadata_hash": meta_result.metadata_hash,
                    "latest_status": info["status"],
                    "latest_epoch": info["epoch"],
                    "latest_slot": info["slot"],
                    "latest_tx_hash": info["tx_hash"],
                    "latest_date": info["source_date"],
                    "fetch_status": "success",
                    "http_status": meta_result.http_status,
                    "last_checked_at": now_iso,
                    "updated_at": now_iso,
                }
                success_count += 1
            else:
                # Never overwrite good with bad
                ex = existing.get(pool_id)
                if ex and ex.get("fetch_status") == "success":
                    profiles[pool_id] = dict(ex)
                    profiles[pool_id]["last_checked_at"] = now_iso
                    # Update on-chain fields
                    profiles[pool_id]["latest_status"] = info["status"]
                    profiles[pool_id]["latest_epoch"] = info["epoch"]
                    profiles[pool_id]["latest_slot"] = info["slot"]
                    profiles[pool_id]["latest_tx_hash"] = info["tx_hash"]
                    profiles[pool_id]["latest_date"] = info["source_date"]
                else:
                    profiles[pool_id] = {
                        "pool_id": pool_id,
                        "ticker": None,
                        "name": None,
                        "description": None,
                        "homepage": None,
                        "metadata_url": None,
                        "metadata_hash": None,
                        "latest_status": info["status"],
                        "latest_epoch": info["epoch"],
                        "latest_slot": info["slot"],
                        "latest_tx_hash": info["tx_hash"],
                        "latest_date": info["source_date"],
                        "fetch_status": "failed",
                        "http_status": meta_result.http_status,
                        "last_checked_at": now_iso,
                        "updated_at": now_iso,
                    }
                fail_count += 1

        logger.info("Resolution: %d success, %d failed", success_count, fail_count)

    # Add pools that weren't in the resolve list (unchanged) but also not in existing
    for pool_id, info in pool_info.items():
        if pool_id not in profiles:
            profiles[pool_id] = {
                "pool_id": pool_id,
                "ticker": None,
                "name": None,
                "description": None,
                "homepage": None,
                "metadata_url": None,
                "metadata_hash": None,
                "latest_status": info["status"],
                "latest_epoch": info["epoch"],
                "latest_slot": info["slot"],
                "latest_tx_hash": info["tx_hash"],
                "latest_date": info["source_date"],
                "fetch_status": "pending",
                "http_status": None,
                "last_checked_at": now_iso,
                "updated_at": now_iso,
            }

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
    for pool_id in sorted(profiles.keys()):
        row = profiles[pool_id]
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

            pools = _read_pools(self.config)
            summary["pools_read"] = len(pools)

            if len(pools) == 0:
                logger.info("No pools to process")
                summary["profiles_after"] = len(existing)
                return summary

            profiles = _build_profiles(pools, existing, self.config, dry_run=dry_run)
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

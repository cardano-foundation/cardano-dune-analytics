"""DRep profile builder: reads registration parquet files, resolves anchor URLs, writes profile."""

import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import AppConfig
from . import register
from .anchor_resolver import resolve_batch

logger = logging.getLogger("yaci_s3.internal.drep_profile")

PROFILE_SCHEMA = pa.schema([
    ("drep_id", pa.string()),
    ("drep_hash", pa.string()),
    ("anchor_url", pa.string()),
    ("anchor_hash", pa.string()),
    ("drep_name", pa.string()),
    ("source_block", pa.int64()),
    ("source_slot", pa.int64()),
    ("source_tx_hash", pa.string()),
    ("source_date", pa.string()),
    ("fetch_status", pa.string()),
    ("http_status", pa.int32()),
    ("last_checked_at", pa.string()),
    ("updated_at", pa.string()),
])


def _profile_dir(config: AppConfig) -> Path:
    return Path(config.base_data_path) / "drep_profile"


def _profile_path(config: AppConfig) -> Path:
    return _profile_dir(config) / "drep_profile.parquet"


def _load_existing_profile(config: AppConfig) -> dict:
    """Load existing profile parquet into a dict keyed by drep_id."""
    path = _profile_path(config)
    if not path.exists():
        return {}

    table = pq.read_table(str(path))
    profiles = {}
    for i in range(len(table)):
        row = {col: table.column(col)[i].as_py() for col in table.column_names}
        profiles[row["drep_id"]] = row
    return profiles


def _read_registrations(config: AppConfig, dates: Optional[List[str]] = None) -> pa.Table:
    """Read drep_registration parquet files using DuckDB.

    Args:
        dates: If None, read all dates (rebuild). Otherwise read specific dates.
    """
    base = config.base_data_path
    reg_dir = Path(base) / "drep_registration"

    if not reg_dir.is_dir():
        logger.warning("drep_registration directory not found: %s", reg_dir)
        return pa.table({})

    from glob import glob as pyglob

    if dates:
        # Read specific dates
        files_list = []
        for d in dates:
            pattern = f"{base}/drep_registration/date={d}/drep_registration-{d}.parquet"
            pattern_alt = f"{base}/drep_registration/date={d}/*.parquet"
            if Path(pattern).exists():
                files_list.append(pattern)
            else:
                files_list.extend(pyglob(pattern_alt))
        if not files_list:
            logger.warning("No registration files found for dates: %s", dates)
            return pa.table({})
    else:
        # Rebuild: read all
        files_list = sorted(pyglob(f"{base}/drep_registration/date=*/drep_registration-*.parquet"))
        if not files_list:
            files_list = sorted(pyglob(f"{base}/drep_registration/date=*/*.parquet"))
        if not files_list:
            logger.warning("No drep_registration parquet files found")
            return pa.table({})

    logger.info("Reading %d registration file(s)", len(files_list))

    conn = duckdb.connect(":memory:")
    try:
        result = conn.execute(
            """
            SELECT drep_id, drep_hash, anchor_url, anchor_hash, block, slot, tx_hash,
                   regexp_extract(filename, 'date=([^/]+)', 1) as source_date
            FROM read_parquet(?, filename=true)
            ORDER BY drep_id, block DESC
            """,
            [files_list],
        ).fetch_arrow_table()
    except Exception as e:
        logger.error("DuckDB read failed: %s", e)
        return pa.table({})
    finally:
        conn.close()

    logger.info("Read %d registration rows", len(result))
    return result


def _build_profiles(
    registrations: pa.Table,
    existing: dict,
    config: AppConfig,
    dry_run: bool = False,
) -> dict:
    """Build/update profiles from registration data.

    Returns updated profiles dict.
    """
    if len(registrations) == 0:
        return existing

    now_iso = datetime.now(timezone.utc).isoformat()

    # Group registrations by drep_id (already sorted by block DESC from query)
    drep_regs = {}
    for i in range(len(registrations)):
        drep_id = registrations.column("drep_id")[i].as_py()
        if drep_id not in drep_regs:
            drep_regs[drep_id] = []
        drep_regs[drep_id].append({
            "drep_id": drep_id,
            "drep_hash": registrations.column("drep_hash")[i].as_py(),
            "anchor_url": registrations.column("anchor_url")[i].as_py(),
            "anchor_hash": registrations.column("anchor_hash")[i].as_py(),
            "block": registrations.column("block")[i].as_py(),
            "slot": registrations.column("slot")[i].as_py(),
            "tx_hash": registrations.column("tx_hash")[i].as_py(),
            "source_date": registrations.column("source_date")[i].as_py(),
        })

    # Determine which dreps need resolution
    dreps_to_resolve = {}  # drep_id -> newest_reg
    dreps_no_url = {}      # drep_id -> newest_reg

    for drep_id, regs in drep_regs.items():
        newest = regs[0]  # Already sorted by block DESC
        ex = existing.get(drep_id)

        # Skip if existing profile has a newer or equal block
        if ex and ex.get("source_block") is not None and newest["block"] <= ex["source_block"]:
            continue

        # Check if any registration has an anchor URL
        has_url = any(
            reg.get("anchor_url") and isinstance(reg["anchor_url"], str) and reg["anchor_url"].strip()
            for reg in regs
        )

        if not has_url:
            dreps_no_url[drep_id] = newest
        else:
            dreps_to_resolve[drep_id] = newest

    logger.info(
        "Profile update: %d dreps to resolve, %d with no URL, %d unchanged",
        len(dreps_to_resolve), len(dreps_no_url),
        len(drep_regs) - len(dreps_to_resolve) - len(dreps_no_url),
    )

    if dry_run:
        logger.info("[DRY RUN] Would resolve %d dreps, set no_url for %d",
                     len(dreps_to_resolve), len(dreps_no_url))
        return existing

    # Resolve metadata via Blockfrost in parallel
    profiles = dict(existing)

    if dreps_to_resolve:
        results = resolve_batch(
            list(dreps_to_resolve.keys()),
            project_id=config.blockfrost_project_id,
            max_workers=config.anchor_max_workers,
        )

        success_count = 0
        fail_count = 0
        for drep_id, anchor_result in results.items():
            newest = dreps_to_resolve[drep_id]

            if anchor_result.success:
                profiles[drep_id] = {
                    "drep_id": drep_id,
                    "drep_hash": newest["drep_hash"],
                    "anchor_url": anchor_result.anchor_url,
                    "anchor_hash": newest["anchor_hash"],
                    "drep_name": anchor_result.drep_name,
                    "source_block": newest["block"],
                    "source_slot": newest["slot"],
                    "source_tx_hash": newest["tx_hash"],
                    "source_date": newest["source_date"],
                    "fetch_status": "success",
                    "http_status": anchor_result.http_status,
                    "last_checked_at": now_iso,
                    "updated_at": now_iso,
                }
                success_count += 1
            else:
                # Never overwrite good with bad
                ex = existing.get(drep_id)
                if ex and ex.get("fetch_status") == "success":
                    # Keep existing good profile, just update last_checked_at
                    profiles[drep_id] = dict(ex)
                    profiles[drep_id]["last_checked_at"] = now_iso
                else:
                    profiles[drep_id] = {
                        "drep_id": drep_id,
                        "drep_hash": newest["drep_hash"],
                        "anchor_url": anchor_result.anchor_url or "",
                        "anchor_hash": newest["anchor_hash"],
                        "drep_name": None,
                        "source_block": newest["block"],
                        "source_slot": newest["slot"],
                        "source_tx_hash": newest["tx_hash"],
                        "source_date": newest["source_date"],
                        "fetch_status": "failed",
                        "http_status": anchor_result.http_status,
                        "last_checked_at": now_iso,
                        "updated_at": now_iso,
                    }
                fail_count += 1

        logger.info("Resolution: %d success, %d failed", success_count, fail_count)

    # Handle no-URL dreps
    for drep_id, newest in dreps_no_url.items():
        ex = existing.get(drep_id)
        if ex and ex.get("fetch_status") == "success":
            # Keep existing good profile
            profiles[drep_id] = dict(ex)
            profiles[drep_id]["last_checked_at"] = now_iso
        else:
            profiles[drep_id] = {
                "drep_id": drep_id,
                "drep_hash": newest["drep_hash"],
                "anchor_url": None,
                "anchor_hash": newest["anchor_hash"],
                "drep_name": None,
                "source_block": newest["block"],
                "source_slot": newest["slot"],
                "source_tx_hash": newest["tx_hash"],
                "source_date": newest["source_date"],
                "fetch_status": "no_url",
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
    for drep_id in sorted(profiles.keys()):
        row = profiles[drep_id]
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

    logger.info("Wrote profile: %d rows -> %s", len(table), out_path)


@register("drep_profile")
class DRepProfileJob:
    """Internal job: build/update the DRep profile lookup table."""

    name = "drep_profile"

    def __init__(self, config: AppConfig):
        self.config = config

    def run(
        self,
        rebuild: bool = False,
        dates: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> dict:
        """Run the profile build/update.

        Args:
            rebuild: If True, rebuild from all registration files (ignores existing profile).
            dates: Specific dates to process. If None and not rebuild, error.
            dry_run: Show what would change without writing.

        Returns summary dict.
        """
        summary = {
            "job": self.name,
            "status": "completed",
            "registrations_read": 0,
            "profiles_before": 0,
            "profiles_after": 0,
        }

        try:
            # Load existing profile (empty if rebuild)
            if rebuild:
                existing = {}
            else:
                existing = _load_existing_profile(self.config)
            summary["profiles_before"] = len(existing)

            # Read registrations
            registrations = _read_registrations(self.config, dates=dates)
            summary["registrations_read"] = len(registrations)

            if len(registrations) == 0:
                logger.info("No registrations to process")
                summary["profiles_after"] = len(existing)
                return summary

            # Build/update profiles
            profiles = _build_profiles(registrations, existing, self.config, dry_run=dry_run)
            summary["profiles_after"] = len(profiles)

            if not dry_run:
                _write_profile(profiles, self.config)

            logger.info(
                "Profile %s complete: %d -> %d profiles (%d registrations)",
                "rebuild" if rebuild else "update",
                summary["profiles_before"], summary["profiles_after"],
                summary["registrations_read"],
            )

        except Exception as e:
            logger.error("Profile job failed: %s", e)
            summary["status"] = "failed"
            summary["error"] = str(e)

        return summary

"""Tests for pool profile builder."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from yaci_s3.config import AppConfig
from yaci_s3.internal.pool_profile import (
    PoolProfileJob,
    _load_existing_profile,
    _profile_path,
    _write_profile,
    PROFILE_SCHEMA,
)


def _make_config(base_path):
    return AppConfig(
        pg_host="", pg_port=5432, pg_db="", pg_user="", pg_password="",
        pg_schema="public", s3_bucket="test", base_data_path=base_path,
        sqlite_path=":memory:", blockfrost_project_id="test_project_id",
        anchor_max_workers=2,
    )


def _write_pool_parquet(base_path, date, rows):
    """Write a pool parquet file for a given date."""
    pool_dir = Path(base_path) / "pool" / f"date={date}"
    pool_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "pool_id": [r["pool_id"] for r in rows],
        "tx_hash": [r.get("tx_hash", f"tx_{r['pool_id']}") for r in rows],
        "cert_index": [0 for _ in rows],
        "tx_index": [0 for _ in rows],
        "status": [r.get("status", "REGISTRATION") for r in rows],
        "amount": [0 for _ in rows],
        "epoch": [r.get("epoch", 500) for r in rows],
        "active_epoch": [r.get("epoch", 500) + 2 for r in rows],
        "retire_epoch": [None for r in rows],
        "registration_slot": [r.get("slot", 1000) for r in rows],
        "slot": [r.get("slot", 1000) for r in rows],
        "block_hash": ["blockhash" for r in rows],
        "block": [r.get("slot", 1000) for r in rows],
        "block_time": [1700000000 for r in rows],
        "date": [date for r in rows],
    })
    pq.write_table(table, str(pool_dir / f"pool-{date}.parquet"))


def test_load_existing_profile_empty():
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        profiles = _load_existing_profile(config)
        assert profiles == {}


def test_write_and_load_profile():
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        profiles = {
            "pool1abc": {
                "pool_id": "pool1abc",
                "ticker": "TAPSY",
                "name": "TapTap Vienna",
                "description": "low fees",
                "homepage": "https://example.com",
                "metadata_url": "https://example.com/meta.json",
                "metadata_hash": "hash1",
                "latest_status": "REGISTRATION",
                "latest_epoch": 500,
                "latest_slot": 1000,
                "latest_tx_hash": "tx1",
                "latest_date": "2024-01-15",
                "fetch_status": "success",
                "http_status": 200,
                "last_checked_at": "2024-01-15T00:00:00",
                "updated_at": "2024-01-15T00:00:00",
            },
        }
        _write_profile(profiles, config)

        path = _profile_path(config)
        assert path.exists()

        loaded = _load_existing_profile(config)
        assert len(loaded) == 1
        assert loaded["pool1abc"]["ticker"] == "TAPSY"
        assert loaded["pool1abc"]["name"] == "TapTap Vienna"


@patch("yaci_s3.internal.pool_profile.resolve_pool_batch")
def test_pool_profile_rebuild(mock_resolve):
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "pool1abc": PoolMetadataResult(
            pool_id="pool1abc", ticker="TAPSY", name="TapTap Vienna",
            description="low fees", homepage="https://example.com",
            metadata_url="https://example.com/meta.json", metadata_hash="hash1",
            success=True, http_status=200,
        ),
        "pool2def": PoolMetadataResult(
            pool_id="pool2def", http_status=404, error="no metadata",
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "pool1abc", "slot": 1000, "epoch": 500},
            {"pool_id": "pool2def", "slot": 2000, "epoch": 510},
        ])

        job = PoolProfileJob(config)
        summary = job.run(rebuild=True)

        assert summary["status"] == "completed"
        assert summary["profiles_after"] == 2

        loaded = _load_existing_profile(config)
        assert loaded["pool1abc"]["ticker"] == "TAPSY"
        assert loaded["pool1abc"]["fetch_status"] == "success"
        assert loaded["pool2def"]["fetch_status"] == "failed"


@patch("yaci_s3.internal.pool_profile.resolve_pool_batch")
def test_pool_profile_never_overwrite_good_with_bad(mock_resolve):
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)

        # Write existing profile with success
        existing = {
            "pool1abc": {
                "pool_id": "pool1abc", "ticker": "TAPSY", "name": "TapTap",
                "description": "old", "homepage": "https://old.com",
                "metadata_url": "https://old.com/meta.json", "metadata_hash": "h1",
                "latest_status": "REGISTRATION", "latest_epoch": 500,
                "latest_slot": 500, "latest_tx_hash": "tx_old",
                "latest_date": "2024-01-10", "fetch_status": "success",
                "http_status": 200, "last_checked_at": "2024-01-10T00:00:00",
                "updated_at": "2024-01-10T00:00:00",
            }
        }
        _write_profile(existing, config)

        # New pool event with higher slot, but resolution will fail
        _write_pool_parquet(base, "2024-01-20", [
            {"pool_id": "pool1abc", "slot": 2000, "epoch": 510},
        ])

        mock_resolve.return_value = {
            "pool1abc": PoolMetadataResult(
                pool_id="pool1abc", http_status=404, error="no metadata",
            ),
        }

        job = PoolProfileJob(config)
        summary = job.run(rebuild=False)

        loaded = _load_existing_profile(config)
        # Should keep old good metadata
        assert loaded["pool1abc"]["ticker"] == "TAPSY"
        assert loaded["pool1abc"]["fetch_status"] == "success"
        # But on-chain fields should update
        assert loaded["pool1abc"]["latest_slot"] == 2000


@patch("yaci_s3.internal.pool_profile.resolve_pool_batch")
def test_pool_profile_dry_run(mock_resolve):
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "pool1abc", "slot": 1000, "epoch": 500},
        ])

        job = PoolProfileJob(config)
        summary = job.run(rebuild=True, dry_run=True)

        assert summary["status"] == "completed"
        assert not _profile_path(config).exists()
        mock_resolve.assert_not_called()


def test_pool_profile_no_pools():
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        job = PoolProfileJob(config)
        summary = job.run(rebuild=True)
        assert summary["status"] == "completed"
        assert summary["pools_read"] == 0

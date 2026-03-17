"""Tests for DRep profile builder."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from yaci_s3.config import AppConfig
from yaci_s3.internal.drep_profile import (
    DRepProfileJob,
    _load_existing_profile,
    _profile_path,
    _write_profile,
    PROFILE_SCHEMA,
)


def _make_config(base_path):
    return AppConfig(
        pg_host="", pg_port=5432, pg_db="", pg_user="", pg_password="",
        pg_schema="public", s3_bucket="test", base_data_path=base_path,
        sqlite_path=":memory:", anchor_timeout=2, ipfs_gateway="https://ipfs.io/ipfs/",
        anchor_max_workers=2,
    )


def _write_registration_parquet(base_path, date, rows):
    """Write a drep_registration parquet file for a given date."""
    reg_dir = Path(base_path) / "drep_registration" / f"date={date}"
    reg_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "drep_id": [r["drep_id"] for r in rows],
        "drep_hash": [r.get("drep_hash", "hash_" + r["drep_id"]) for r in rows],
        "anchor_url": [r.get("anchor_url") for r in rows],
        "anchor_hash": [r.get("anchor_hash") for r in rows],
        "block": [r["block"] for r in rows],
        "slot": [r.get("slot", r["block"] * 20) for r in rows],
        "tx_hash": [r.get("tx_hash", f"tx_{r['drep_id']}_{r['block']}") for r in rows],
    })
    pq.write_table(table, str(reg_dir / f"drep_registration-{date}.parquet"))


def test_load_existing_profile_empty():
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        profiles = _load_existing_profile(config)
        assert profiles == {}


def test_write_and_load_profile():
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        profiles = {
            "drep1abc": {
                "drep_id": "drep1abc",
                "drep_hash": "hash1",
                "anchor_url": "https://example.com/meta.json",
                "anchor_hash": "ahash1",
                "drep_name": "Alice",
                "source_block": 100,
                "source_slot": 2000,
                "source_tx_hash": "tx1",
                "source_date": "2024-01-15",
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
        assert loaded["drep1abc"]["drep_name"] == "Alice"
        assert loaded["drep1abc"]["fetch_status"] == "success"


def test_write_profile_atomic(tmp_path):
    """Profile write should be atomic (temp file then replace)."""
    config = _make_config(str(tmp_path))
    profiles = {
        "drep1": {
            "drep_id": "drep1", "drep_hash": "h1", "anchor_url": None,
            "anchor_hash": None, "drep_name": None, "source_block": 1,
            "source_slot": 20, "source_tx_hash": "tx1", "source_date": "2024-01-01",
            "fetch_status": "no_url", "http_status": None,
            "last_checked_at": "2024-01-01T00:00:00", "updated_at": "2024-01-01T00:00:00",
        }
    }
    _write_profile(profiles, config)
    # Verify file exists and is valid parquet
    table = pq.read_table(str(_profile_path(config)))
    assert len(table) == 1
    assert table.column("drep_id")[0].as_py() == "drep1"


@patch("yaci_s3.internal.drep_profile.resolve_batch")
def test_drep_profile_rebuild(mock_resolve_batch):
    """Test full rebuild reads registrations and resolves anchors."""
    from yaci_s3.internal.anchor_resolver import AnchorResult

    mock_resolve_batch.return_value = {
        "drep1abc": AnchorResult(
            anchor_url="https://example.com/meta.json",
            drep_name="Alice DRep",
            success=True,
            http_status=200,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_registration_parquet(base, "2024-01-15", [
            {"drep_id": "drep1abc", "anchor_url": "https://example.com/meta.json",
             "anchor_hash": "ahash1", "block": 100},
        ])
        _write_registration_parquet(base, "2024-01-16", [
            {"drep_id": "drep2def", "anchor_url": None,
             "anchor_hash": None, "block": 200},
        ])

        job = DRepProfileJob(config)
        summary = job.run(rebuild=True)

        assert summary["status"] == "completed"
        assert summary["profiles_after"] == 2

        loaded = _load_existing_profile(config)
        assert "drep1abc" in loaded
        assert loaded["drep1abc"]["drep_name"] == "Alice DRep"
        assert loaded["drep1abc"]["fetch_status"] == "success"
        assert "drep2def" in loaded
        assert loaded["drep2def"]["fetch_status"] == "no_url"


@patch("yaci_s3.internal.drep_profile.resolve_batch")
def test_drep_profile_never_overwrite_good_with_bad(mock_resolve_batch):
    """If an existing profile has success status and new resolution fails, keep the old one."""
    from yaci_s3.internal.anchor_resolver import AnchorResult

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)

        # Write existing profile with success
        existing = {
            "drep1abc": {
                "drep_id": "drep1abc", "drep_hash": "hash1",
                "anchor_url": "https://old.com/meta.json", "anchor_hash": "ahash1",
                "drep_name": "Alice", "source_block": 50, "source_slot": 1000,
                "source_tx_hash": "tx_old", "source_date": "2024-01-10",
                "fetch_status": "success", "http_status": 200,
                "last_checked_at": "2024-01-10T00:00:00", "updated_at": "2024-01-10T00:00:00",
            }
        }
        _write_profile(existing, config)

        # New registration with higher block, but resolution will fail
        _write_registration_parquet(base, "2024-01-20", [
            {"drep_id": "drep1abc", "anchor_url": "https://new-broken.com/meta.json",
             "anchor_hash": "ahash2", "block": 200},
        ])

        mock_resolve_batch.return_value = {
            "drep1abc": AnchorResult(
                anchor_url="https://new-broken.com/meta.json",
                success=False, error="HTTP 404", http_status=404,
            ),
        }

        job = DRepProfileJob(config)
        summary = job.run(rebuild=False, dates=["2024-01-20"])

        loaded = _load_existing_profile(config)
        # Should keep old good profile
        assert loaded["drep1abc"]["drep_name"] == "Alice"
        assert loaded["drep1abc"]["fetch_status"] == "success"
        assert loaded["drep1abc"]["anchor_url"] == "https://old.com/meta.json"


@patch("yaci_s3.internal.drep_profile.resolve_batch")
def test_drep_profile_dry_run(mock_resolve_batch):
    """Dry run should not write profile file."""
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_registration_parquet(base, "2024-01-15", [
            {"drep_id": "drep1abc", "anchor_url": "https://example.com/meta.json",
             "anchor_hash": "ahash1", "block": 100},
        ])

        job = DRepProfileJob(config)
        summary = job.run(rebuild=True, dry_run=True)

        assert summary["status"] == "completed"
        assert not _profile_path(config).exists()
        mock_resolve_batch.assert_not_called()


def test_drep_profile_no_registrations():
    """Empty registration dir should produce empty result."""
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        job = DRepProfileJob(config)
        summary = job.run(rebuild=True)
        assert summary["status"] == "completed"
        assert summary["registrations_read"] == 0

"""Tests for off-chain pool data exporter."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from yaci_s3.config import AppConfig
from yaci_s3.external.off_chain_pool_data import (
    OffChainPoolDataExporter,
    _read_all_pool_hashes,
    _read_pool_hashes_since,
    _read_already_exported,
    _read_max_versions,
    _get_last_export_date,
    SCHEMA,
)


def _make_config(base_path):
    return AppConfig(
        pg_host="", pg_port=5432, pg_db="", pg_user="", pg_password="",
        pg_schema="public", s3_bucket="test", base_data_path=base_path,
        sqlite_path=":memory:", blockfrost_project_id="test_project_id",
        anchor_max_workers=2,
    )


def _write_pool_parquet(base_path, date, rows):
    """Write a pool_registration parquet file for a given date."""
    pool_dir = Path(base_path) / "pool_registration" / f"date={date}"
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
    pq.write_table(table, str(pool_dir / f"pool_registration-{date}.parquet"))


def _write_export_parquet(base_path, date, pool_hashes, versions=None):
    """Write a fake off_chain_pool_data export parquet."""
    if versions is None:
        versions = [1 for _ in pool_hashes]
    export_dir = Path(base_path) / "off_chain_pool_data" / f"date={date}"
    export_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "pool_hash": pool_hashes,
        "pool_id": [f"pool{h[:4]}" for h in pool_hashes],
        "ticker": ["TK" for _ in pool_hashes],
        "name": ["name" for _ in pool_hashes],
        "description": [None for _ in pool_hashes],
        "homepage": [None for _ in pool_hashes],
        "metadata_url": [None for _ in pool_hashes],
        "metadata_hash": [None for _ in pool_hashes],
        "fetched_at": [datetime(2024, 1, 1, tzinfo=timezone.utc) for _ in pool_hashes],
        "version": versions,
    }, schema=SCHEMA)
    pq.write_table(table, str(export_dir / f"off_chain_pool_data-{date}.1.parquet"))


# --- Unit tests for helper functions ---

def test_read_all_pool_hashes_empty():
    with tempfile.TemporaryDirectory() as base:
        assert _read_all_pool_hashes(base) == []


def test_read_all_pool_hashes():
    with tempfile.TemporaryDirectory() as base:
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc", "slot": 1000, "epoch": 500},
            {"pool_id": "hash2def", "slot": 2000, "epoch": 510},
            {"pool_id": "hash1abc", "slot": 3000, "epoch": 520},  # duplicate
        ])
        result = _read_all_pool_hashes(base)
        assert result == ["hash1abc", "hash2def"]


def test_read_pool_hashes_since():
    with tempfile.TemporaryDirectory() as base:
        _write_pool_parquet(base, "2024-01-10", [
            {"pool_id": "hash1abc"},
        ])
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc"},  # updated
            {"pool_id": "hash2def"},  # new
        ])

        # Since 2024-01-10 → only partitions after that date
        result = _read_pool_hashes_since(base, "2024-01-10")
        assert result == {"hash1abc", "hash2def"}

        # Since 2024-01-15 → nothing after
        result = _read_pool_hashes_since(base, "2024-01-15")
        assert result == set()


def test_read_already_exported_empty():
    with tempfile.TemporaryDirectory() as base:
        assert _read_already_exported(base) == set()


def test_read_already_exported():
    with tempfile.TemporaryDirectory() as base:
        _write_export_parquet(base, "2024-01-15", ["hash1abc", "hash2def"])
        result = _read_already_exported(base)
        assert result == {"hash1abc", "hash2def"}


def test_get_last_export_date():
    with tempfile.TemporaryDirectory() as base:
        assert _get_last_export_date(base) is None

        _write_export_parquet(base, "2024-01-10", ["hash1abc"])
        _write_export_parquet(base, "2024-01-15", ["hash2def"])
        assert _get_last_export_date(base) == "2024-01-15"


# --- Integration tests for fetch_data ---

@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_rebuild(mock_resolve):
    """Rebuild mode resolves all pools, ignoring previous exports."""
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash1abc": PoolMetadataResult(
            pool_hash="hash1abc", pool_id="pool1bech32", ticker="TAPSY",
            name="TapTap Vienna", description="low fees",
            homepage="https://example.com",
            metadata_url="https://example.com/meta.json", metadata_hash="hash1",
            success=True,
        ),
        "hash2def": PoolMetadataResult(
            pool_hash="hash2def", error="no metadata",
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc"},
            {"pool_id": "hash2def"},
        ])
        _write_export_parquet(base, "2024-01-10", ["hash1abc"])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        exporter._rebuild = True
        table = exporter.fetch_data()

        assert table is not None
        assert len(table) == 1
        assert table.column("pool_hash")[0].as_py() == "hash1abc"
        # Both pools should have been sent to resolve (rebuild)
        resolved_hashes = mock_resolve.call_args[0][0]
        assert set(resolved_hashes) == {"hash1abc", "hash2def"}


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_incremental_new_pool(mock_resolve):
    """Incremental mode picks up pools not in previous exports."""
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash2def": PoolMetadataResult(
            pool_hash="hash2def", pool_id="pool2bech32", ticker="NEW",
            name="New Pool", success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-10", [{"pool_id": "hash1abc"}])
        _write_pool_parquet(base, "2024-01-15", [{"pool_id": "hash2def"}])
        _write_export_parquet(base, "2024-01-10", ["hash1abc"])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()

        assert table is not None
        assert len(table) == 1
        assert table.column("pool_hash")[0].as_py() == "hash2def"


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_incremental_updated_pool(mock_resolve):
    """Incremental mode picks up pools that re-registered after last export."""
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash1abc": PoolMetadataResult(
            pool_hash="hash1abc", pool_id="pool1bech32", ticker="UPDATED",
            name="Updated Pool", success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        # Pool registered on Jan 10
        _write_pool_parquet(base, "2024-01-10", [{"pool_id": "hash1abc"}])
        # Exported on Jan 10
        _write_export_parquet(base, "2024-01-10", ["hash1abc"])
        # Pool re-registered (updated) on Jan 15 — after last export
        _write_pool_parquet(base, "2024-01-15", [{"pool_id": "hash1abc"}])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()

        assert table is not None
        assert len(table) == 1
        assert table.column("pool_hash")[0].as_py() == "hash1abc"
        assert table.column("ticker")[0].as_py() == "UPDATED"


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_incremental_nothing_new(mock_resolve):
    """Incremental returns None when no new or updated pools."""
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-10", [{"pool_id": "hash1abc"}])
        _write_export_parquet(base, "2024-01-10", ["hash1abc"])
        # No new pool partitions after export date

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()

        assert table is None
        mock_resolve.assert_not_called()


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_all_fail(mock_resolve):
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash1abc": PoolMetadataResult(pool_hash="hash1abc", error="no metadata"),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [{"pool_id": "hash1abc"}])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()

        assert table is None


def test_fetch_data_no_pools():
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()
        assert table is None


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_validate(mock_resolve):
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash1abc": PoolMetadataResult(
            pool_hash="hash1abc", pool_id="pool1bech32", ticker="TAPSY",
            name="TapTap Vienna", success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [{"pool_id": "hash1abc"}])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()
        assert exporter.validate(table)


def test_run_sets_rebuild():
    """Verify run(rebuild=True) sets _rebuild flag."""
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        db = MagicMock()
        db.start_external_run.return_value = 1
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)

        exporter.run(dry_run=True, rebuild=True)
        assert exporter._rebuild is True

        exporter.run(dry_run=True, rebuild=False)
        assert exporter._rebuild is False


# --- Version column tests ---

def test_read_max_versions_empty():
    with tempfile.TemporaryDirectory() as base:
        assert _read_max_versions(base) == {}


def test_read_max_versions():
    with tempfile.TemporaryDirectory() as base:
        _write_export_parquet(base, "2024-01-10", ["hash1abc", "hash2def"], [1, 1])
        _write_export_parquet(base, "2024-01-15", ["hash1abc"], [2])

        result = _read_max_versions(base)
        assert result == {"hash1abc": 2, "hash2def": 1}


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_version_rebuild_always_one(mock_resolve):
    """Rebuild mode sets version=1 for all rows."""
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash1abc": PoolMetadataResult(
            pool_hash="hash1abc", pool_id="pool1bech32", ticker="TAPSY",
            name="Pool", success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [{"pool_id": "hash1abc"}])
        # Pre-existing export with version=3
        _write_export_parquet(base, "2024-01-10", ["hash1abc"], [3])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        exporter._rebuild = True
        table = exporter.fetch_data()

        assert table is not None
        assert table.column("version").to_pylist() == [1]


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_version_incremental_increments(mock_resolve):
    """Incremental mode increments version from existing max."""
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash1abc": PoolMetadataResult(
            pool_hash="hash1abc", pool_id="pool1bech32", ticker="UPDATED",
            name="Updated Pool", success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-10", [{"pool_id": "hash1abc"}])
        _write_export_parquet(base, "2024-01-10", ["hash1abc"], [2])
        # Pool re-registered after export
        _write_pool_parquet(base, "2024-01-15", [{"pool_id": "hash1abc"}])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()

        assert table is not None
        assert table.column("version").to_pylist() == [3]


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_version_new_pool_gets_one(mock_resolve):
    """A brand new pool gets version=1 in incremental mode."""
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash2def": PoolMetadataResult(
            pool_hash="hash2def", pool_id="pool2bech32", ticker="NEW",
            name="New Pool", success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-10", [{"pool_id": "hash1abc"}])
        _write_pool_parquet(base, "2024-01-15", [{"pool_id": "hash2def"}])
        _write_export_parquet(base, "2024-01-10", ["hash1abc"], [1])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()

        assert table is not None
        assert table.column("version").to_pylist() == [1]

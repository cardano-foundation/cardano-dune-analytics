"""Tests for off-chain pool data exporter."""

import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from yaci_s3.config import AppConfig
from yaci_s3.external.off_chain_pool_data import (
    OffChainPoolDataExporter,
    _read_pool_hashes,
    _read_already_exported,
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


def _write_export_parquet(base_path, date, pool_hashes):
    """Write a fake off_chain_pool_data export parquet."""
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
        "fetched_at": ["2024-01-15T00:00:00+00:00" for _ in pool_hashes],
    }, schema=SCHEMA)
    pq.write_table(table, str(export_dir / f"off_chain_pool_data-{date}.1.parquet"))


def test_read_pool_hashes_empty():
    with tempfile.TemporaryDirectory() as base:
        result = _read_pool_hashes(base)
        assert result == []


def test_read_pool_hashes():
    with tempfile.TemporaryDirectory() as base:
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc", "slot": 1000, "epoch": 500},
            {"pool_id": "hash2def", "slot": 2000, "epoch": 510},
            {"pool_id": "hash1abc", "slot": 3000, "epoch": 520},  # duplicate
        ])
        result = _read_pool_hashes(base)
        assert result == ["hash1abc", "hash2def"]


def test_read_already_exported_empty():
    with tempfile.TemporaryDirectory() as base:
        result = _read_already_exported(base)
        assert result == set()


def test_read_already_exported():
    with tempfile.TemporaryDirectory() as base:
        _write_export_parquet(base, "2024-01-15", ["hash1abc", "hash2def"])
        result = _read_already_exported(base)
        assert result == {"hash1abc", "hash2def"}


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
            {"pool_id": "hash1abc", "slot": 1000, "epoch": 500},
            {"pool_id": "hash2def", "slot": 2000, "epoch": 510},
        ])
        # Pre-existing export for hash1abc — rebuild should still re-resolve it
        _write_export_parquet(base, "2024-01-10", ["hash1abc"])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        exporter._rebuild = True
        table = exporter.fetch_data()

        assert table is not None
        assert len(table) == 1
        assert table.column("pool_hash")[0].as_py() == "hash1abc"
        # Both pools should have been sent to resolve (rebuild mode)
        resolved_hashes = mock_resolve.call_args[0][0]
        assert set(resolved_hashes) == {"hash1abc", "hash2def"}


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_incremental(mock_resolve):
    """Incremental mode skips already-exported pools."""
    from yaci_s3.internal.anchor_resolver import PoolMetadataResult

    mock_resolve.return_value = {
        "hash2def": PoolMetadataResult(
            pool_hash="hash2def", pool_id="pool2bech32", ticker="NEW",
            name="New Pool", success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc", "slot": 1000, "epoch": 500},
            {"pool_id": "hash2def", "slot": 2000, "epoch": 510},
        ])
        # hash1abc was already exported
        _write_export_parquet(base, "2024-01-10", ["hash1abc"])

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        # Default is incremental (rebuild=False)
        table = exporter.fetch_data()

        assert table is not None
        assert len(table) == 1
        assert table.column("pool_hash")[0].as_py() == "hash2def"
        # Only hash2def should have been sent to resolve
        resolved_hashes = mock_resolve.call_args[0][0]
        assert resolved_hashes == ["hash2def"]


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_incremental_nothing_new(mock_resolve):
    """Incremental mode returns None when all pools already exported."""
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc", "slot": 1000, "epoch": 500},
        ])
        _write_export_parquet(base, "2024-01-10", ["hash1abc"])

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
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc", "slot": 1000, "epoch": 500},
        ])

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
            name="TapTap Vienna", description="low fees",
            homepage="https://example.com",
            metadata_url="https://example.com/meta.json", metadata_hash="hash1",
            success=True,
        ),
    }

    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)
        _write_pool_parquet(base, "2024-01-15", [
            {"pool_id": "hash1abc", "slot": 1000, "epoch": 500},
        ])

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

        # No pool data, so it will return early — just verify rebuild flag is set
        exporter.run(dry_run=True, rebuild=True)
        assert exporter._rebuild is True

        exporter.run(dry_run=True, rebuild=False)
        assert exporter._rebuild is False

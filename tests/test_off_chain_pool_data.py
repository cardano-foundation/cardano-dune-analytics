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


@patch("yaci_s3.external.off_chain_pool_data.resolve_pool_batch")
def test_fetch_data_success_and_failure(mock_resolve):
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

        db = MagicMock()
        uploader = MagicMock()
        exporter = OffChainPoolDataExporter(config, db, uploader)
        table = exporter.fetch_data()

        # Only successful pool should appear
        assert table is not None
        assert len(table) == 1
        assert table.column("pool_hash")[0].as_py() == "hash1abc"
        assert table.column("ticker")[0].as_py() == "TAPSY"
        assert table.column("pool_id")[0].as_py() == "pool1bech32"
        assert table.column("fetched_at")[0].as_py() is not None


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

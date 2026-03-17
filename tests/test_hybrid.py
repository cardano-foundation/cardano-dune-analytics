"""Tests for hybrid exporters (drep_dist_enriched)."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from yaci_s3.config import AppConfig
from yaci_s3.db import TrackingDB
from yaci_s3.hybrid.drep_dist_enriched import DRepDistEnrichedExporter
from yaci_s3.internal.drep_profile import _write_profile, PROFILE_SCHEMA


def _make_config(base_path, db_path=":memory:"):
    return AppConfig(
        pg_host="", pg_port=5432, pg_db="", pg_user="", pg_password="",
        pg_schema="public", s3_bucket="test", base_data_path=base_path,
        sqlite_path=db_path, anchor_timeout=2, ipfs_gateway="https://ipfs.io/ipfs/",
        anchor_max_workers=2,
    )


def _write_drep_dist(base_path, epoch, rows):
    """Write a drep_dist parquet for a given epoch."""
    dist_dir = Path(base_path) / "drep_dist" / f"epoch={epoch}"
    dist_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "drep_hash": [r["drep_hash"] for r in rows],
        "drep_type": [r.get("drep_type", "key") for r in rows],
        "drep_id": [r["drep_id"] for r in rows],
        "amount": [r.get("amount", 1000000) for r in rows],
        "epoch": [int(epoch)] * len(rows),
        "active_until": [r.get("active_until", int(epoch) + 10) for r in rows],
        "expiry": [r.get("expiry", int(epoch) + 20) for r in rows],
        "update_datetime": [r.get("update_datetime", "2024-01-15T00:00:00") for r in rows],
    })
    pq.write_table(table, str(dist_dir / f"drep_dist-epoch-{epoch}.parquet"))


def test_enrich_adds_columns():
    """Enrichment should add anchor_url and drep_name columns."""
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)

        # Write profile
        profiles = {
            "drep1abc": {
                "drep_id": "drep1abc", "drep_hash": "hash1",
                "anchor_url": "https://example.com/meta.json", "anchor_hash": "ah1",
                "drep_name": "Alice", "source_block": 100, "source_slot": 2000,
                "source_tx_hash": "tx1", "source_date": "2024-01-15",
                "fetch_status": "success", "http_status": 200,
                "last_checked_at": "2024-01-15T00:00:00", "updated_at": "2024-01-15T00:00:00",
            },
        }
        _write_profile(profiles, config)

        # Create source table
        source = pa.table({
            "drep_hash": ["hash1", "hash2"],
            "drep_type": ["key", "key"],
            "drep_id": ["drep1abc", "drep2def"],
            "amount": [1000000, 2000000],
            "epoch": [500, 500],
            "active_until": [510, 510],
            "expiry": [520, 520],
            "update_datetime": ["2024-01-15T00:00:00", "2024-01-15T00:00:00"],
        })

        db = TrackingDB(":memory:")
        uploader = MagicMock()
        exporter = DRepDistEnrichedExporter(config, db, uploader)

        enriched = exporter.enrich(source, "500")

        assert "anchor_url" in enriched.column_names
        assert "drep_name" in enriched.column_names
        assert len(enriched) == 2

        # drep1abc has a profile match
        assert enriched.column("drep_name")[0].as_py() == "Alice"
        assert enriched.column("anchor_url")[0].as_py() == "https://example.com/meta.json"

        # drep2def has no profile match
        assert enriched.column("drep_name")[1].as_py() is None
        assert enriched.column("anchor_url")[1].as_py() is None

        db.close()


def test_enrich_no_profile_file():
    """Without a profile file, all names/urls should be None."""
    with tempfile.TemporaryDirectory() as base:
        config = _make_config(base)

        source = pa.table({
            "drep_hash": ["hash1"],
            "drep_type": ["key"],
            "drep_id": ["drep1abc"],
            "amount": [1000000],
            "epoch": [500],
            "active_until": [510],
            "expiry": [520],
            "update_datetime": ["2024-01-15T00:00:00"],
        })

        db = TrackingDB(":memory:")
        uploader = MagicMock()
        exporter = DRepDistEnrichedExporter(config, db, uploader)

        enriched = exporter.enrich(source, "500")
        assert enriched.column("drep_name")[0].as_py() is None
        assert enriched.column("anchor_url")[0].as_py() is None
        db.close()


def test_hybrid_run_dry_run():
    """Dry run should enrich and write locally but not upload."""
    with tempfile.TemporaryDirectory() as base:
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        try:
            config = _make_config(base, db_path)

            # Write profile
            profiles = {
                "drep1abc": {
                    "drep_id": "drep1abc", "drep_hash": "hash1",
                    "anchor_url": "https://example.com/meta.json", "anchor_hash": "ah1",
                    "drep_name": "Alice", "source_block": 100, "source_slot": 2000,
                    "source_tx_hash": "tx1", "source_date": "2024-01-15",
                    "fetch_status": "success", "http_status": 200,
                    "last_checked_at": "2024-01-15T00:00:00", "updated_at": "2024-01-15T00:00:00",
                },
            }
            _write_profile(profiles, config)

            # Write drep_dist source
            _write_drep_dist(base, "500", [
                {"drep_hash": "hash1", "drep_id": "drep1abc"},
                {"drep_hash": "hash2", "drep_id": "drep2def"},
            ])

            db = TrackingDB(db_path)
            uploader = MagicMock()
            exporter = DRepDistEnrichedExporter(config, db, uploader)

            summary = exporter.run(dry_run=True)

            assert summary["scanned"] == 1
            assert summary["enriched"] == 1
            assert summary["uploaded"] == 0

            # Local enriched file should exist
            enriched_path = Path(base) / "drep_dist_enriched" / "epoch=500" / "drep_dist_enriched-epoch-500.parquet"
            assert enriched_path.exists()

            enriched = pq.ParquetFile(str(enriched_path)).read()
            assert "drep_name" in enriched.column_names
            assert "anchor_url" in enriched.column_names
            assert len(enriched) == 2

            db.close()
        finally:
            os.unlink(db_path)


def test_hybrid_skips_already_uploaded():
    """Should skip partitions already tracked as uploaded."""
    with tempfile.TemporaryDirectory() as base:
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        try:
            config = _make_config(base, db_path)

            _write_drep_dist(base, "500", [
                {"drep_hash": "hash1", "drep_id": "drep1abc"},
            ])

            db = TrackingDB(db_path)
            # Mark epoch 500 as already uploaded for drep_dist_enriched
            from yaci_s3.models import UploadRecord
            db.record_upload(UploadRecord(
                exporter="drep_dist_enriched", partition_value="500",
                s3_key="drep_dist_enriched/500/drep_dist_enriched-epoch-500.parquet",
                file_name="drep_dist_enriched-epoch-500.parquet",
                row_count=1, min_slot=None, max_slot=None, file_size=100,
            ))

            uploader = MagicMock()
            exporter = DRepDistEnrichedExporter(config, db, uploader)
            summary = exporter.run(dry_run=True)

            assert summary["scanned"] == 1
            assert summary["skipped_existing"] == 1
            assert summary["enriched"] == 0

            db.close()
        finally:
            os.unlink(db_path)


def test_hybrid_partition_filter():
    """Should only process partitions in the filter."""
    with tempfile.TemporaryDirectory() as base:
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        try:
            config = _make_config(base, db_path)

            _write_drep_dist(base, "500", [{"drep_hash": "h1", "drep_id": "d1"}])
            _write_drep_dist(base, "501", [{"drep_hash": "h2", "drep_id": "d2"}])

            db = TrackingDB(db_path)
            uploader = MagicMock()
            exporter = DRepDistEnrichedExporter(config, db, uploader)

            summary = exporter.run(dry_run=True, partition_filter={"501"})
            assert summary["enriched"] == 1

            enriched_500 = Path(base) / "drep_dist_enriched" / "epoch=500"
            enriched_501 = Path(base) / "drep_dist_enriched" / "epoch=501"
            assert not enriched_500.exists()
            assert enriched_501.exists()

            db.close()
        finally:
            os.unlink(db_path)

"""Tests for SQLite tracking database."""

import os
import tempfile

import pytest

from yaci_s3.db import TrackingDB
from yaci_s3.models import ParquetStats, PgStats, UploadRecord, ValidationResult


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    tracking = TrackingDB(path)
    yield tracking
    tracking.close()
    os.unlink(path)


def test_record_and_query_upload(db):
    db.record_upload(UploadRecord(
        exporter="block",
        partition_value="2024-01-15",
        s3_key="block/date=2024-01-15/block-2024-01-15.parquet",
        file_name="block-2024-01-15.parquet",
        row_count=100,
        min_slot=1000,
        max_slot=2000,
        file_size=5000,
    ))
    uploaded = db.get_uploaded_partitions("block")
    assert "2024-01-15" in uploaded
    assert "2024-01-16" not in uploaded


def test_record_validation_error(db):
    result = ValidationResult(
        exporter="block",
        partition_value="2024-01-15",
        is_valid=False,
        pq_stats=ParquetStats(row_count=100, min_slot=1000, max_slot=2000),
        pg_stats=PgStats(row_count=90, min_slot=1000, max_slot=2000),
        error_details="Row count mismatch",
    )
    db.record_validation_error(result)
    cursor = db.conn.execute("SELECT COUNT(*) FROM validation_errors")
    assert cursor.fetchone()[0] == 1


def test_rebuild_from_s3(db):
    objects = [
        {"exporter": "block", "partition_value": "2024-01-15",
         "s3_key": "block/date=2024-01-15/block-2024-01-15.parquet",
         "file_name": "block-2024-01-15.parquet", "file_size": 5000},
        {"exporter": "block", "partition_value": "2024-01-16",
         "s3_key": "block/date=2024-01-16/block-2024-01-16.parquet",
         "file_name": "block-2024-01-16.parquet", "file_size": 6000},
    ]
    db.rebuild_from_s3(objects)
    uploaded = db.get_uploaded_partitions("block")
    assert uploaded == {"2024-01-15", "2024-01-16"}


def test_upsert_upload(db):
    """Test that recording the same partition twice updates rather than duplicates."""
    record = UploadRecord(
        exporter="block", partition_value="2024-01-15",
        s3_key="block/date=2024-01-15/block-2024-01-15.parquet",
        file_name="block-2024-01-15.parquet", row_count=100,
        min_slot=1000, max_slot=2000, file_size=5000,
    )
    db.record_upload(record)
    record.row_count = 200
    db.record_upload(record)
    cursor = db.conn.execute("SELECT COUNT(*) FROM uploads WHERE exporter='block'")
    assert cursor.fetchone()[0] == 1

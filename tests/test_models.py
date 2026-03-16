"""Tests for models and config."""

import json
import os
import tempfile

import pytest


def test_exporter_def():
    from yaci_s3.models import ExporterDef
    e = ExporterDef(name="block", pg_table="block", slot_column="slot", partition_type="daily")
    assert e.name == "block"
    assert e.group == "dune"


def test_partition_info():
    from yaci_s3.models import PartitionInfo
    p = PartitionInfo(
        exporter="block",
        partition_value="2024-01-15",
        partition_type="daily",
        file_path="/tmp/block-2024-01-15.parquet",
        file_size=1024,
    )
    assert p.partition_value == "2024-01-15"


def test_load_exporters():
    from yaci_s3.config import load_exporters
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"exporters": [
            {"name": "block", "pg_table": "block", "slot_column": "slot", "partition_type": "daily"},
        ]}, f)
        f.flush()
        exporters = load_exporters(f.name)
    os.unlink(f.name)
    assert "block" in exporters
    assert exporters["block"].pg_table == "block"


def test_load_config_missing_env():
    from yaci_s3.config import load_config
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as ef:
        json.dump({"exporters": []}, ef)
        ef.flush()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as envf:
        envf.write("")
        envf.flush()
    # Clear env vars that might be set
    for k in ["PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD", "S3_BUCKET", "BASE_DATA_PATH"]:
        os.environ.pop(k, None)
    with pytest.raises(ValueError, match="Missing required env vars"):
        load_config(envf.name, ef.name)
    os.unlink(ef.name)
    os.unlink(envf.name)

"""Tests for filesystem scanner."""

import os
import tempfile
from pathlib import Path

from yaci_s3.models import ExporterDef
from yaci_s3.scanner import scan_exporter


def test_scan_daily_exporter():
    with tempfile.TemporaryDirectory() as base:
        exp_dir = Path(base) / "block"
        for date in ["2024-01-15", "2024-01-16"]:
            d = exp_dir / f"date={date}"
            d.mkdir(parents=True)
            (d / f"block-{date}.parquet").write_bytes(b"fake parquet")

        exporter = ExporterDef(name="block", pg_table="block", slot_column="slot", partition_type="daily")
        partitions = scan_exporter(base, exporter)
        assert len(partitions) == 2
        assert partitions[0].partition_value == "2024-01-15"
        assert partitions[1].partition_value == "2024-01-16"


def test_scan_epoch_exporter():
    with tempfile.TemporaryDirectory() as base:
        exp_dir = Path(base) / "reward"
        for epoch in ["500", "501"]:
            d = exp_dir / f"epoch={epoch}"
            d.mkdir(parents=True)
            (d / f"reward-epoch-{epoch}.parquet").write_bytes(b"fake parquet")

        exporter = ExporterDef(name="reward", pg_table="reward", slot_column="earned_epoch", partition_type="epoch")
        partitions = scan_exporter(base, exporter)
        assert len(partitions) == 2
        assert partitions[0].partition_value == "500"


def test_scan_ignores_bare_dirs():
    """Bare date/epoch dirs without prefix should be ignored."""
    with tempfile.TemporaryDirectory() as base:
        exp_dir = Path(base) / "block"
        d = exp_dir / "2024-01-15"
        d.mkdir(parents=True)
        (d / "block-2024-01-15.parquet").write_bytes(b"fake parquet")

        exporter = ExporterDef(name="block", pg_table="block", slot_column="slot", partition_type="daily")
        partitions = scan_exporter(base, exporter)
        assert len(partitions) == 0


def test_scan_missing_directory():
    with tempfile.TemporaryDirectory() as base:
        exporter = ExporterDef(name="nonexistent", pg_table="x", slot_column="x", partition_type="daily")
        partitions = scan_exporter(base, exporter)
        assert partitions == []

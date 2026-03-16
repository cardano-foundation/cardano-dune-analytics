"""Tests for S3 uploader (key parsing only, no real S3)."""

from yaci_s3.uploader import S3Uploader, _build_s3_key
from yaci_s3.models import PartitionInfo


def test_build_s3_key_daily():
    p = PartitionInfo(
        exporter="block", partition_value="2024-01-15",
        partition_type="daily", file_path="/tmp/block-2024-01-15.parquet",
    )
    assert _build_s3_key(p) == "block/2024-01-15/block-2024-01-15.parquet"


def test_build_s3_key_epoch():
    p = PartitionInfo(
        exporter="reward", partition_value="500",
        partition_type="epoch", file_path="/tmp/reward-epoch-500.parquet",
    )
    assert _build_s3_key(p) == "reward/500/reward-epoch-500.parquet"


def test_parse_s3_key_daily():
    result = S3Uploader._parse_s3_key("block/2024-01-15/block-2024-01-15.parquet")
    assert result["exporter"] == "block"
    assert result["partition_value"] == "2024-01-15"


def test_parse_s3_key_epoch():
    result = S3Uploader._parse_s3_key("reward/500/reward-epoch-500.parquet")
    assert result["exporter"] == "reward"
    assert result["partition_value"] == "500"


def test_parse_s3_key_invalid():
    assert S3Uploader._parse_s3_key("random/path/file.txt") is None

"""Data models for yaci-s3."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ExporterDef:
    """Definition of a parquet exporter."""
    name: str
    pg_table: str
    slot_column: str
    partition_type: str  # "daily" or "epoch"
    group: str = "dune"


@dataclass
class PartitionInfo:
    """A discovered local partition."""
    exporter: str
    partition_value: str  # "2024-01-15" for daily, "500" for epoch
    partition_type: str   # "daily" or "epoch"
    file_path: str
    file_size: int = 0


@dataclass
class ParquetStats:
    """Stats read from a parquet file via DuckDB."""
    row_count: int
    min_slot: Optional[int] = None
    max_slot: Optional[int] = None


@dataclass
class PgStats:
    """Stats read from PostgreSQL."""
    row_count: int
    min_slot: Optional[int] = None
    max_slot: Optional[int] = None


@dataclass
class ValidationResult:
    """Result of validating a partition."""
    exporter: str
    partition_value: str
    is_valid: bool
    pq_stats: Optional[ParquetStats] = None
    pg_stats: Optional[PgStats] = None
    error_details: Optional[str] = None


@dataclass
class UploadRecord:
    """Record of a successful upload."""
    exporter: str
    partition_value: str
    s3_key: str
    file_name: str
    row_count: int
    min_slot: Optional[int]
    max_slot: Optional[int]
    file_size: int
    status: str = "completed"

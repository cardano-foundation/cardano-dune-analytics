"""Validation: DuckDB parquet stats + PostgreSQL count/slot validation."""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import duckdb
import psycopg2

from .config import AppConfig
from .models import ExporterDef, ParquetStats, PartitionInfo, PgStats, ValidationResult

logger = logging.getLogger("yaci_s3.validator")

PG_MAX_RETRIES = 3
PG_RETRY_DELAY = 5


def read_parquet_stats(partition: PartitionInfo, exporter: ExporterDef) -> ParquetStats:
    """Read row count and slot range from a parquet file using DuckDB."""
    conn = duckdb.connect()
    try:
        if exporter.partition_type == "daily":
            result = conn.execute(
                f"SELECT COUNT(*), MIN({exporter.slot_column}), MAX({exporter.slot_column}) "
                f"FROM read_parquet('{partition.file_path}')"
            ).fetchone()
            return ParquetStats(
                row_count=result[0],
                min_slot=result[1],
                max_slot=result[2],
            )
        else:
            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{partition.file_path}')"
            ).fetchone()
            return ParquetStats(row_count=result[0])
    finally:
        conn.close()


def _pg_connect(config: AppConfig):
    """Connect to PostgreSQL with retries."""
    last_err = None
    for attempt in range(1, PG_MAX_RETRIES + 1):
        try:
            return psycopg2.connect(config.pg_dsn)
        except psycopg2.OperationalError as e:
            last_err = e
            logger.warning("PG connection attempt %d/%d failed: %s", attempt, PG_MAX_RETRIES, e)
            if attempt < PG_MAX_RETRIES:
                time.sleep(PG_RETRY_DELAY)
    raise last_err


def _validate_daily(
    partition: PartitionInfo,
    exporter: ExporterDef,
    pq_stats: ParquetStats,
    config: AppConfig,
) -> ValidationResult:
    """Validate a daily partition against PG."""
    schema = config.pg_schema
    date = partition.partition_value
    next_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    conn = _pg_connect(config)
    try:
        with conn.cursor() as cur:
            # Get slot range from block table for this date
            cur.execute(
                f"SELECT MIN(slot), MAX(slot), COUNT(*) FROM {schema}.block "
                f"WHERE block_time >= EXTRACT(EPOCH FROM %s::timestamp)::bigint "
                f"AND block_time < EXTRACT(EPOCH FROM %s::timestamp)::bigint",
                (date, next_date),
            )
            block_row = cur.fetchone()
            pg_block_min_slot, pg_block_max_slot, _ = block_row

            if pg_block_min_slot is None:
                return ValidationResult(
                    exporter=exporter.name,
                    partition_value=date,
                    is_valid=False,
                    pq_stats=pq_stats,
                    error_details=f"No blocks found in PG for date {date}",
                )

            # Get count from the exporter's table using parquet slot range
            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.{exporter.pg_table} "
                f"WHERE {exporter.slot_column} >= %s AND {exporter.slot_column} <= %s",
                (pq_stats.min_slot, pq_stats.max_slot),
            )
            pg_count = cur.fetchone()[0]
    finally:
        conn.close()

    pg_stats = PgStats(
        row_count=pg_count,
        min_slot=pg_block_min_slot,
        max_slot=pg_block_max_slot,
    )

    errors = []
    if pq_stats.row_count != pg_count:
        errors.append(
            f"Row count mismatch: parquet={pq_stats.row_count}, pg={pg_count}"
        )
    if pq_stats.min_slot < pg_block_min_slot or pq_stats.max_slot > pg_block_max_slot:
        errors.append(
            f"Slot range outside block range: pq=[{pq_stats.min_slot},{pq_stats.max_slot}], "
            f"blocks=[{pg_block_min_slot},{pg_block_max_slot}]"
        )

    if errors:
        return ValidationResult(
            exporter=exporter.name,
            partition_value=date,
            is_valid=False,
            pq_stats=pq_stats,
            pg_stats=pg_stats,
            error_details="; ".join(errors),
        )

    return ValidationResult(
        exporter=exporter.name,
        partition_value=date,
        is_valid=True,
        pq_stats=pq_stats,
        pg_stats=pg_stats,
    )


def _validate_epoch(
    partition: PartitionInfo,
    exporter: ExporterDef,
    pq_stats: ParquetStats,
    config: AppConfig,
) -> ValidationResult:
    """Validate an epoch partition against PG."""
    schema = config.pg_schema
    epoch = int(partition.partition_value)

    conn = _pg_connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {schema}.{exporter.pg_table} "
                f"WHERE {exporter.slot_column} = %s",
                (epoch,),
            )
            pg_count = cur.fetchone()[0]
    finally:
        conn.close()

    pg_stats = PgStats(row_count=pg_count)

    if pq_stats.row_count != pg_count:
        return ValidationResult(
            exporter=exporter.name,
            partition_value=partition.partition_value,
            is_valid=False,
            pq_stats=pq_stats,
            pg_stats=pg_stats,
            error_details=f"Row count mismatch: parquet={pq_stats.row_count}, pg={pg_count}",
        )

    return ValidationResult(
        exporter=exporter.name,
        partition_value=partition.partition_value,
        is_valid=True,
        pq_stats=pq_stats,
        pg_stats=pg_stats,
    )


def validate_partition(
    partition: PartitionInfo,
    exporter: ExporterDef,
    config: AppConfig,
) -> ValidationResult:
    """Validate a partition: read parquet stats, then check against PG."""
    try:
        pq_stats = read_parquet_stats(partition, exporter)
    except Exception as e:
        logger.error("Failed to read parquet %s: %s", partition.file_path, e)
        return ValidationResult(
            exporter=exporter.name,
            partition_value=partition.partition_value,
            is_valid=False,
            error_details=f"DuckDB read error: {e}",
        )

    try:
        if exporter.partition_type == "daily":
            return _validate_daily(partition, exporter, pq_stats, config)
        else:
            return _validate_epoch(partition, exporter, pq_stats, config)
    except Exception as e:
        logger.error("PG validation failed for %s/%s: %s", exporter.name, partition.partition_value, e)
        return ValidationResult(
            exporter=exporter.name,
            partition_value=partition.partition_value,
            is_valid=False,
            pq_stats=pq_stats,
            error_details=f"PG validation error: {e}",
        )

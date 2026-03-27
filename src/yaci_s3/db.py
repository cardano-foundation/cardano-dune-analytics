"""SQLite tracking database for uploads and validation errors."""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Set

from .models import UploadRecord, ValidationResult

logger = logging.getLogger("yaci_s3.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS uploads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter TEXT NOT NULL,
    partition_value TEXT NOT NULL,
    s3_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    min_slot INTEGER,
    max_slot INTEGER,
    file_size INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    UNIQUE(exporter, partition_value)
);

CREATE TABLE IF NOT EXISTS upload_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter TEXT NOT NULL,
    partition_value TEXT NOT NULL,
    file_path TEXT,
    file_size INTEGER,
    error_details TEXT,
    attempts INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS validation_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter TEXT NOT NULL,
    partition_value TEXT NOT NULL,
    pq_count INTEGER,
    pg_count INTEGER,
    pq_min_slot INTEGER,
    pq_max_slot INTEGER,
    pg_min_slot INTEGER,
    pg_max_slot INTEGER,
    error_details TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS external_exporter_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter TEXT NOT NULL,
    run_started_at TEXT NOT NULL,
    run_completed_at TEXT,
    records_fetched INTEGER,
    records_exported INTEGER,
    status TEXT NOT NULL DEFAULT 'running',
    error_details TEXT,
    source_data_watermark TEXT
);

CREATE TABLE IF NOT EXISTS contract_registry_state (
    source TEXT PRIMARY KEY,
    last_commit_sha TEXT NOT NULL,
    last_checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contract_registry_hashes (
    script_hash TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);
"""


class TrackingDB:
    """SQLite database for tracking uploads and validation errors."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        # Migrate existing databases: add source_data_watermark column
        try:
            self.conn.execute("ALTER TABLE external_exporter_runs ADD COLUMN source_data_watermark TEXT")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists

    def get_uploaded_partitions(self, exporter: str) -> Set[str]:
        """Get set of partition values already uploaded for an exporter."""
        cursor = self.conn.execute(
            "SELECT partition_value FROM uploads WHERE exporter = ? AND status = 'completed'",
            (exporter,),
        )
        return {row["partition_value"] for row in cursor.fetchall()}

    def record_upload(self, record: UploadRecord):
        """Record a successful upload."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """INSERT OR REPLACE INTO uploads
               (exporter, partition_value, s3_key, file_name, row_count, min_slot, max_slot, file_size, uploaded_at, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.exporter,
                record.partition_value,
                record.s3_key,
                record.file_name,
                record.row_count,
                record.min_slot,
                record.max_slot,
                record.file_size,
                now,
                record.status,
            ),
        )
        self.conn.commit()

    def record_validation_error(self, result: ValidationResult):
        """Record a validation error."""
        now = datetime.now(timezone.utc).isoformat()
        pq = result.pq_stats
        pg = result.pg_stats
        self.conn.execute(
            """INSERT INTO validation_errors
               (exporter, partition_value, pq_count, pg_count, pq_min_slot, pq_max_slot, pg_min_slot, pg_max_slot, error_details, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                result.exporter,
                result.partition_value,
                pq.row_count if pq else None,
                pg.row_count if pg else None,
                pq.min_slot if pq else None,
                pq.max_slot if pq else None,
                pg.min_slot if pg else None,
                pg.max_slot if pg else None,
                result.error_details,
                now,
            ),
        )
        self.conn.commit()

    def record_upload_error(self, exporter: str, partition_value: str,
                           file_path: str, file_size: int,
                           error_details: str, attempts: int):
        """Record a failed upload."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO upload_errors
               (exporter, partition_value, file_path, file_size, error_details, attempts, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (exporter, partition_value, file_path, file_size, error_details, attempts, now),
        )
        self.conn.commit()

    def get_failed_partitions(self, exporter_filter: Optional[str] = None) -> dict:
        """Get partitions that failed upload or validation, grouped by exporter.

        Returns dict: {exporter_name: set(partition_values)}
        """
        failures = {}

        # Upload errors
        if exporter_filter:
            cursor = self.conn.execute(
                "SELECT DISTINCT exporter, partition_value FROM upload_errors WHERE exporter = ?",
                (exporter_filter,),
            )
        else:
            cursor = self.conn.execute(
                "SELECT DISTINCT exporter, partition_value FROM upload_errors"
            )
        for row in cursor.fetchall():
            failures.setdefault(row["exporter"], set()).add(row["partition_value"])

        # Validation errors
        if exporter_filter:
            cursor = self.conn.execute(
                "SELECT DISTINCT exporter, partition_value FROM validation_errors WHERE exporter = ?",
                (exporter_filter,),
            )
        else:
            cursor = self.conn.execute(
                "SELECT DISTINCT exporter, partition_value FROM validation_errors"
            )
        for row in cursor.fetchall():
            failures.setdefault(row["exporter"], set()).add(row["partition_value"])

        # Exclude partitions that have since been successfully uploaded
        for exporter_name in list(failures.keys()):
            uploaded = self.get_uploaded_partitions(exporter_name)
            failures[exporter_name] -= uploaded
            if not failures[exporter_name]:
                del failures[exporter_name]

        return failures

    def clear_errors_for_partition(self, exporter: str, partition_value: str):
        """Remove error records for a partition (called after successful retry)."""
        self.conn.execute(
            "DELETE FROM upload_errors WHERE exporter = ? AND partition_value = ?",
            (exporter, partition_value),
        )
        self.conn.execute(
            "DELETE FROM validation_errors WHERE exporter = ? AND partition_value = ?",
            (exporter, partition_value),
        )
        self.conn.commit()

    def rebuild_from_s3(self, s3_objects: List[dict]):
        """Rebuild uploads table from S3 object listing.

        Each dict should have: exporter, partition_value, s3_key, file_name, file_size
        """
        self.conn.execute("DELETE FROM uploads")
        now = datetime.now(timezone.utc).isoformat()
        for obj in s3_objects:
            self.conn.execute(
                """INSERT OR REPLACE INTO uploads
                   (exporter, partition_value, s3_key, file_name, row_count, min_slot, max_slot, file_size, uploaded_at, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    obj["exporter"],
                    obj["partition_value"],
                    obj["s3_key"],
                    obj["file_name"],
                    obj.get("row_count", 0),
                    obj.get("min_slot"),
                    obj.get("max_slot"),
                    obj.get("file_size", 0),
                    now,
                    "completed",
                ),
            )
        self.conn.commit()
        logger.info("Rebuilt uploads table with %d records", len(s3_objects))

    # --- External exporter helpers ---

    def get_next_suffix(self, exporter: str, date_str: str) -> int:
        """Get the next .N suffix for an external exporter partition.

        Counts existing uploads where partition_value starts with date_str
        (e.g., '2024-01-15.1', '2024-01-15.2') and returns count + 1.
        """
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM uploads WHERE exporter = ? AND partition_value LIKE ?",
            (exporter, f"{date_str}.%"),
        )
        count = cursor.fetchone()[0]
        return count + 1

    def start_external_run(self, exporter: str) -> int:
        """Record the start of an external exporter run. Returns the run ID."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            "INSERT INTO external_exporter_runs (exporter, run_started_at, status) VALUES (?, ?, 'running')",
            (exporter, now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def complete_external_run(self, run_id: int, records_fetched: int,
                              records_exported: int, status: str = "completed",
                              error_details: str = None,
                              source_data_watermark: str = None):
        """Update an external exporter run as completed or failed."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE external_exporter_runs
               SET run_completed_at = ?, records_fetched = ?, records_exported = ?,
                   status = ?, error_details = ?, source_data_watermark = ?
               WHERE id = ?""",
            (now, records_fetched, records_exported, status, error_details,
             source_data_watermark, run_id),
        )
        self.conn.commit()

    def get_last_source_watermark(self, exporter: str) -> Optional[str]:
        """Get the source_data_watermark from the last successful run."""
        cursor = self.conn.execute(
            """SELECT source_data_watermark FROM external_exporter_runs
               WHERE exporter = ? AND status = 'completed' AND source_data_watermark IS NOT NULL
               ORDER BY id DESC LIMIT 1""",
            (exporter,),
        )
        row = cursor.fetchone()
        return row["source_data_watermark"] if row else None

    def get_contract_registry_state(self, source: str) -> Optional[str]:
        """Get the last commit SHA for a contract registry source."""
        cursor = self.conn.execute(
            "SELECT last_commit_sha FROM contract_registry_state WHERE source = ?",
            (source,),
        )
        row = cursor.fetchone()
        return row["last_commit_sha"] if row else None

    def update_contract_registry_state(self, source: str, commit_sha: str):
        """Update the commit SHA for a contract registry source."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """INSERT OR REPLACE INTO contract_registry_state
               (source, last_commit_sha, last_checked_at)
               VALUES (?, ?, ?)""",
            (source, commit_sha, now),
        )
        self.conn.commit()

    def get_known_script_hashes(self) -> Set[str]:
        """Get all known script hashes from the contract registry."""
        cursor = self.conn.execute("SELECT script_hash FROM contract_registry_hashes")
        return {row["script_hash"] for row in cursor.fetchall()}

    def get_script_hash_sources(self) -> dict:
        """Get mapping of script_hash -> source for priority checks."""
        cursor = self.conn.execute("SELECT script_hash, source FROM contract_registry_hashes")
        return {row["script_hash"]: row["source"] for row in cursor.fetchall()}

    def add_script_hashes(self, hashes: List[dict]):
        """Add new script hashes to the contract registry.

        Each dict should have: script_hash, source
        """
        now = datetime.now(timezone.utc).isoformat()
        for h in hashes:
            self.conn.execute(
                """INSERT OR IGNORE INTO contract_registry_hashes
                   (script_hash, source, first_seen_at)
                   VALUES (?, ?, ?)""",
                (h["script_hash"], h["source"], now),
            )
        self.conn.commit()

    def close(self):
        self.conn.close()

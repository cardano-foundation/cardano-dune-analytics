"""Configuration loading from .env and exporters.json."""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

from .models import ExporterDef


@dataclass
class AppConfig:
    """Application configuration."""
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    pg_schema: str
    s3_bucket: str
    base_data_path: str
    sqlite_path: str
    aws_profile: str = ""
    exporters: Dict[str, ExporterDef] = field(default_factory=dict)
    github_token: str = ""
    minswap_request_delay: float = 1.0
    minswap_max_retries: int = 5

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


def load_exporters(path: str) -> Dict[str, ExporterDef]:
    """Load exporter definitions from exporters.json."""
    with open(path, "r") as f:
        data = json.load(f)
    exporters = {}
    for entry in data["exporters"]:
        exp = ExporterDef(
            name=entry["name"],
            pg_table=entry["pg_table"],
            slot_column=entry["slot_column"],
            partition_type=entry["partition_type"],
            group=entry.get("group", "dune"),
        )
        exporters[exp.name] = exp
    return exporters


def load_config(env_path: str = ".env", exporters_path: str = "exporters.json",
                require_pg: bool = True) -> AppConfig:
    """Load full config from .env file and exporters.json.

    Args:
        require_pg: If False, PG credentials are not required (for external exporters).
    """
    load_dotenv(env_path)

    if require_pg:
        required = ["PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD", "S3_BUCKET", "BASE_DATA_PATH"]
    else:
        required = ["S3_BUCKET", "BASE_DATA_PATH"]

    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")

    exporters = {}
    if os.path.exists(exporters_path):
        exporters = load_exporters(exporters_path)

    return AppConfig(
        pg_host=os.getenv("PG_HOST", ""),
        pg_port=int(os.getenv("PG_PORT", "5432")),
        pg_db=os.getenv("PG_DB", ""),
        pg_user=os.getenv("PG_USER", ""),
        pg_password=os.getenv("PG_PASSWORD", ""),
        pg_schema=os.getenv("PG_SCHEMA", "public"),
        s3_bucket=os.getenv("S3_BUCKET"),
        base_data_path=os.getenv("BASE_DATA_PATH"),
        sqlite_path=os.getenv("SQLITE_PATH", "./uploads.db"),
        aws_profile=os.getenv("AWS_PROFILE", ""),
        exporters=exporters,
        github_token=os.getenv("GITHUB_TOKEN", ""),
        minswap_request_delay=float(os.getenv("MINSWAP_REQUEST_DELAY", "1.0")),
        minswap_max_retries=int(os.getenv("MINSWAP_MAX_RETRIES", "5")),
    )

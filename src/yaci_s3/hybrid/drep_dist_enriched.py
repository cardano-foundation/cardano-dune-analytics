"""Enriched DRep distribution: joins drep_dist with drep_profile."""

import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import AppConfig
from ..db import TrackingDB
from ..uploader import S3Uploader
from . import register
from .base import HybridExporter

logger = logging.getLogger("yaci_s3.hybrid.drep_dist_enriched")


@register("drep_dist_enriched")
class DRepDistEnrichedExporter(HybridExporter):
    """Join drep_dist epoch files with drep_profile to produce enriched output."""

    name = "drep_dist_enriched"
    source_exporter_name = "drep_dist"
    source_partition_type = "epoch"

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        super().__init__(config, db, uploader)
        self._profile_cache = None

    def _load_profile(self) -> dict:
        """Load drep_profile into a lookup dict keyed by drep_id."""
        if self._profile_cache is not None:
            return self._profile_cache

        profile_path = Path(self.config.base_data_path) / "drep_profile" / "drep_profile.parquet"
        if not profile_path.exists():
            logger.warning("drep_profile.parquet not found at %s — enrichment will have empty names", profile_path)
            self._profile_cache = {}
            return self._profile_cache

        table = pq.read_table(str(profile_path), columns=["drep_id", "anchor_url", "drep_name"])
        profile = {}
        for i in range(len(table)):
            drep_id = table.column("drep_id")[i].as_py()
            profile[drep_id] = {
                "anchor_url": table.column("anchor_url")[i].as_py(),
                "drep_name": table.column("drep_name")[i].as_py(),
            }

        logger.info("Loaded drep_profile: %d entries", len(profile))
        self._profile_cache = profile
        return self._profile_cache

    def enrich(self, source_table: pa.Table, partition_value: str) -> pa.Table:
        """Join drep_dist with drep_profile on drep_id."""
        profile = self._load_profile()

        # Build enrichment columns
        drep_ids = source_table.column("drep_id")
        anchor_urls = []
        drep_names = []

        for i in range(len(drep_ids)):
            drep_id = drep_ids[i].as_py()
            entry = profile.get(drep_id)
            if entry:
                anchor_urls.append(entry["anchor_url"])
                drep_names.append(entry["drep_name"])
            else:
                anchor_urls.append(None)
                drep_names.append(None)

        # Cast any dictionary-encoded columns back to plain types before appending
        new_fields = []
        new_columns = []
        for i, field in enumerate(source_table.schema):
            col = source_table.column(i)
            if pa.types.is_dictionary(field.type):
                col = col.cast(field.type.value_type)
                field = pa.field(field.name, field.type.value_type, nullable=field.nullable)
            new_fields.append(field)
            new_columns.append(col)
        source_table = pa.table(new_columns, schema=pa.schema(new_fields))

        # Append new columns to existing table
        enriched = source_table.append_column("anchor_url", pa.array(anchor_urls, type=pa.string()))
        enriched = enriched.append_column("drep_name", pa.array(drep_names, type=pa.string()))

        matched = sum(1 for n in drep_names if n is not None)
        logger.info(
            "[%s] Epoch %s: %d rows, %d matched profile (%d unmatched)",
            self.name, partition_value, len(enriched), matched, len(enriched) - matched,
        )

        return enriched

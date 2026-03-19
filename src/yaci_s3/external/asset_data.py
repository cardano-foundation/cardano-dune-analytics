"""Asset data exporter - fetches verified token data from Minswap API."""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import requests

from . import register
from .base import ExternalExporter
from ..config import AppConfig
from ..db import TrackingDB
from ..uploader import S3Uploader

logger = logging.getLogger("yaci_s3.external.asset_data")

MINSWAP_API_URL = "https://api-mainnet-prod.minswap.org/v1/assets/metrics"
HEX56_RE = re.compile(r"^[0-9a-fA-F]{56}$")
INT64_MAX = (2**63) - 1
INT64_MIN = -(2**63)


def _safe_int(value) -> int:
    """Clamp a value to int64 range, defaulting to 0 for non-numeric."""
    try:
        v = int(value)
        return max(INT64_MIN, min(INT64_MAX, v))
    except (TypeError, ValueError, OverflowError):
        return 0


def _safe_float(value) -> float:
    """Convert to float, defaulting to 0.0 for non-numeric."""
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0


class MinswapClient:
    """Client for the Minswap asset metrics API with cursor pagination."""

    def __init__(self, request_delay: float = 1.0, max_retries: int = 5):
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def fetch_all_assets(self) -> list:
        """Fetch all asset metrics using search_after pagination.

        Returns a list of asset metric dicts.
        """
        all_assets = []
        search_after = None
        page = 0

        while True:
            page += 1
            payload = {"limit": 100, "only_verified": True}
            if search_after is not None:
                payload["search_after"] = search_after

            data = self._request_with_retry(payload)
            if data is None:
                raise RuntimeError(f"Failed to fetch page {page} after retries")

            assets = data.get("asset_metrics", [])
            if not assets:
                logger.info("Page %d: empty response, pagination complete", page)
                break

            all_assets.extend(assets)
            logger.info("Page %d: fetched %d assets (total: %d)", page, len(assets), len(all_assets))

            search_after = data.get("search_after")
            if search_after is None:
                logger.info("No search_after returned, pagination complete")
                break

            time.sleep(self.request_delay)

        return all_assets

    def _request_with_retry(self, payload: dict) -> Optional[dict]:
        """POST to the Minswap API with retry and backoff on 429."""
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(MINSWAP_API_URL, json=payload, timeout=30)

                if resp.status_code == 429:
                    logger.warning(
                        "Rate limited (429), waiting 5s (attempt %d/%d)",
                        attempt, self.max_retries,
                    )
                    time.sleep(5)
                    continue

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as e:
                logger.warning("Request failed (attempt %d/%d): %s", attempt, self.max_retries, e)
                if attempt < self.max_retries:
                    delay = min(10 * (2 ** (attempt - 1)), 120)
                    time.sleep(delay)

        return None


@register("asset_data")
class AssetDataExporter(ExternalExporter):
    """Exports verified token data from the Minswap API."""

    name = "asset_data"
    multi_snapshot = True

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        super().__init__(config, db, uploader)
        self.client = MinswapClient(
            request_delay=config.minswap_request_delay,
            max_retries=config.minswap_max_retries,
        )

    def fetch_data(self) -> Optional[pa.Table]:
        """Fetch all verified token data from Minswap."""
        raw_assets = self.client.fetch_all_assets()

        if not raw_assets:
            return None

        now = datetime.now(timezone.utc).isoformat()

        rows = []
        for metric in raw_assets:
            asset = metric.get("asset", {})
            metadata = asset.get("metadata") or {}
            rows.append({
                "policy_id": asset.get("currency_symbol", ""),
                "token_name": asset.get("token_name", ""),
                "name": metadata.get("name", ""),
                "url": metadata.get("url", ""),
                "ticker": metadata.get("ticker", ""),
                "decimals": _safe_int(metadata.get("decimals", 0)),
                "created_at": metric.get("created_at", ""),
                "categories": ",".join(metric.get("categories") or []),
                "price": _safe_float(metric.get("price", 0.0)),
                "fetched_at": now,
            })

        schema = pa.schema([
            ("policy_id", pa.string()),
            ("token_name", pa.string()),
            ("name", pa.string()),
            ("url", pa.string()),
            ("ticker", pa.string()),
            ("decimals", pa.int64()),
            ("created_at", pa.string()),
            ("categories", pa.string()),
            ("price", pa.float64()),
            ("fetched_at", pa.string()),
        ])

        arrays = {col.name: pa.array([r[col.name] for r in rows], type=col.type)
                  for col in schema}
        return pa.table(arrays, schema=schema)

    def validate(self, table: pa.Table) -> bool:
        """Validate asset data: row count > 0, unique composite key, policy_id format."""
        if len(table) == 0:
            logger.error("Validation failed: empty table")
            return False

        # Check composite key uniqueness (policy_id, token_name)
        policy_ids = table.column("policy_id").to_pylist()
        token_names = table.column("token_name").to_pylist()
        keys = set()
        duplicates = 0
        for pid, tn in zip(policy_ids, token_names):
            key = (pid, tn)
            if key in keys:
                duplicates += 1
            else:
                keys.add(key)

        if duplicates > 0:
            logger.error("Validation failed: %d duplicate (policy_id, token_name) pairs", duplicates)
            return False

        # Check policy_id format: 56 hex chars or empty string
        invalid_pids = 0
        for pid in policy_ids:
            if pid != "" and not HEX56_RE.match(pid):
                invalid_pids += 1

        if invalid_pids > 0:
            logger.error(
                "Validation failed: %d policy_ids are not 56 hex chars or empty", invalid_pids,
            )
            return False

        logger.info("Validation passed: %d rows, %d unique keys", len(table), len(keys))
        return True

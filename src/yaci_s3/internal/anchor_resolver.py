"""Blockfrost-based DRep metadata resolver."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger("yaci_s3.internal.anchor_resolver")

BLOCKFROST_BASE = "https://cardano-mainnet.blockfrost.io/api/v0"


@dataclass
class AnchorResult:
    """Result of resolving a single DRep's metadata."""
    anchor_url: str
    drep_name: Optional[str] = None
    http_status: Optional[int] = None
    success: bool = False
    error: Optional[str] = None


def resolve_via_blockfrost(
    drep_id: str,
    project_id: str,
    timeout: int = 30,
) -> AnchorResult:
    """Fetch DRep metadata from Blockfrost API.

    Calls GET /governance/dreps/{drep_id}/metadata and extracts givenName.
    """
    url = f"{BLOCKFROST_BASE}/governance/dreps/{drep_id}/metadata"
    try:
        resp = requests.get(
            url,
            headers={"project_id": project_id},
            timeout=timeout,
        )
        result = AnchorResult(anchor_url="", http_status=resp.status_code)

        if resp.status_code == 404:
            result.error = "no metadata"
            return result

        if resp.status_code == 429:
            result.error = "rate limited"
            return result

        if resp.status_code != 200:
            result.error = f"HTTP {resp.status_code}"
            return result

        data = resp.json()

        # Extract anchor_url from response
        anchor_url = data.get("url", "")
        result.anchor_url = anchor_url

        # Extract givenName: try json_metadata.body.givenName first, then json_metadata.givenName
        given_name = None
        json_metadata = data.get("json_metadata")
        if isinstance(json_metadata, dict):
            body = json_metadata.get("body")
            if isinstance(body, dict):
                given_name = body.get("givenName")
            if not given_name:
                given_name = json_metadata.get("givenName")

        if given_name and isinstance(given_name, str) and given_name.strip():
            result.drep_name = given_name.strip()
            result.success = True
        else:
            result.error = "no givenName in metadata"

        return result

    except requests.exceptions.Timeout:
        return AnchorResult(anchor_url="", error="timeout")
    except requests.exceptions.ConnectionError:
        return AnchorResult(anchor_url="", error="connection error")
    except Exception as e:
        return AnchorResult(anchor_url="", error=str(e))


def resolve_batch(
    drep_ids: list,
    project_id: str,
    max_workers: int = 5,
    timeout: int = 30,
) -> dict:
    """Resolve metadata for multiple DReps via Blockfrost in parallel.

    Args:
        drep_ids: List of drep_id strings
        project_id: Blockfrost project_id for authentication
        max_workers: Thread pool size
        timeout: HTTP timeout per request

    Returns:
        {drep_id: AnchorResult}
    """
    results = {}
    rate_limit_delay = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for drep_id in drep_ids:
            future = pool.submit(
                resolve_via_blockfrost, drep_id,
                project_id=project_id, timeout=timeout,
            )
            futures[future] = drep_id

        for future in as_completed(futures):
            drep_id = futures[future]
            try:
                result = future.result()
                results[drep_id] = result

                # If rate limited, add delay for subsequent requests
                if result.error == "rate limited":
                    rate_limit_delay = min(rate_limit_delay + 0.5, 5.0)
                    logger.warning("Rate limited on %s, backing off %.1fs", drep_id, rate_limit_delay)
                    time.sleep(rate_limit_delay)
                elif rate_limit_delay > 0:
                    rate_limit_delay = max(rate_limit_delay - 0.1, 0)

            except Exception as e:
                logger.error("Unexpected error resolving %s: %s", drep_id, e)
                results[drep_id] = AnchorResult(anchor_url="", error=str(e))

    return results

"""Blockfrost-based DRep metadata resolver."""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger("yaci_s3.internal.anchor_resolver")

BLOCKFROST_BASE = "https://cardano-mainnet.blockfrost.io/api/v0"
RATE_LIMIT_RPS = 10  # Blockfrost allows 10 requests/second
MAX_RETRIES = 3       # Retry 429s up to 3 times


@dataclass
class AnchorResult:
    """Result of resolving a single DRep's metadata."""
    anchor_url: str
    drep_name: Optional[str] = None
    http_status: Optional[int] = None
    success: bool = False
    error: Optional[str] = None


class _RateLimiter:
    """Thread-safe token bucket rate limiter.

    Allows a burst of `burst` requests, refilling at `rate` tokens/second.
    """

    def __init__(self, rate: float, burst: int):
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        """Block until a token is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            time.sleep(1.0 / self._rate)


def resolve_via_blockfrost(
    drep_id: str,
    project_id: str,
    timeout: int = 30,
    rate_limiter: Optional[_RateLimiter] = None,
) -> AnchorResult:
    """Fetch DRep metadata from Blockfrost API.

    Calls GET /governance/dreps/{drep_id}/metadata and extracts givenName.
    Retries on 429 (rate limit) up to MAX_RETRIES times.
    """
    url = f"{BLOCKFROST_BASE}/governance/dreps/{drep_id}/metadata"

    for attempt in range(1, MAX_RETRIES + 1):
        if rate_limiter:
            rate_limiter.acquire()

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
                if attempt < MAX_RETRIES:
                    backoff = 2 ** attempt
                    logger.debug("Rate limited on %s, retry %d/%d in %ds", drep_id, attempt, MAX_RETRIES, backoff)
                    time.sleep(backoff)
                    continue
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

    return AnchorResult(anchor_url="", error="rate limited")


def resolve_batch(
    drep_ids: list,
    project_id: str,
    max_workers: int = 10,
    timeout: int = 30,
) -> dict:
    """Resolve metadata for multiple DReps via Blockfrost in parallel.

    Uses a token-bucket rate limiter (10 req/s, 500 burst) to stay within
    Blockfrost's rate limits. Retries 429s with exponential backoff.

    Args:
        drep_ids: List of drep_id strings
        project_id: Blockfrost project_id for authentication
        max_workers: Thread pool size
        timeout: HTTP timeout per request

    Returns:
        {drep_id: AnchorResult}
    """
    results = {}
    limiter = _RateLimiter(rate=RATE_LIMIT_RPS, burst=500)

    logger.info("Resolving %d dreps via Blockfrost (%d workers, %d req/s limit)",
                len(drep_ids), max_workers, RATE_LIMIT_RPS)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                resolve_via_blockfrost, drep_id,
                project_id=project_id, timeout=timeout,
                rate_limiter=limiter,
            ): drep_id
            for drep_id in drep_ids
        }

        for future in as_completed(futures):
            drep_id = futures[future]
            try:
                results[drep_id] = future.result()
            except Exception as e:
                logger.error("Unexpected error resolving %s: %s", drep_id, e)
                results[drep_id] = AnchorResult(anchor_url="", error=str(e))

    return results

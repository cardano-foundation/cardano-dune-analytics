"""Blockfrost API client for resolving off-chain metadata (DRep, pool, etc.)."""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Optional

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


@dataclass
class PoolMetadataResult:
    """Result of resolving a single pool's off-chain metadata."""
    pool_hash: str
    pool_id: Optional[str] = None  # bech32 pool_id from Blockfrost
    ticker: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    homepage: Optional[str] = None
    metadata_url: Optional[str] = None
    metadata_hash: Optional[str] = None
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


def _blockfrost_get(
    url: str,
    project_id: str,
    timeout: int = 30,
    rate_limiter: Optional[_RateLimiter] = None,
) -> tuple:
    """Make a rate-limited GET request to Blockfrost with retry on 429.

    Returns (response_json_or_None, http_status, error_or_None).
    """
    for attempt in range(1, MAX_RETRIES + 1):
        if rate_limiter:
            rate_limiter.acquire()

        try:
            resp = requests.get(
                url,
                headers={"project_id": project_id},
                timeout=timeout,
            )

            if resp.status_code == 404:
                return None, 404, "no metadata"

            if resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    backoff = 2 ** attempt
                    logger.debug("Rate limited on %s, retry %d/%d in %ds", url, attempt, MAX_RETRIES, backoff)
                    time.sleep(backoff)
                    continue
                return None, 429, "rate limited"

            if resp.status_code != 200:
                return None, resp.status_code, f"HTTP {resp.status_code}"

            return resp.json(), 200, None

        except requests.exceptions.Timeout:
            return None, None, "timeout"
        except requests.exceptions.ConnectionError:
            return None, None, "connection error"
        except Exception as e:
            return None, None, str(e)

    return None, None, "rate limited"


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
    data, status, error = _blockfrost_get(url, project_id, timeout, rate_limiter)

    if error:
        return AnchorResult(anchor_url="", http_status=status, error=error)

    result = AnchorResult(anchor_url=data.get("url", ""), http_status=status)

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


def resolve_pool_metadata(
    pool_id: str,
    project_id: str,
    timeout: int = 30,
    rate_limiter: Optional[_RateLimiter] = None,
) -> PoolMetadataResult:
    """Fetch pool off-chain metadata from Blockfrost API.

    Calls GET /pools/{pool_id}/metadata and extracts ticker, name, description, homepage.
    """
    url = f"{BLOCKFROST_BASE}/pools/{pool_id}/metadata"
    data, status, error = _blockfrost_get(url, project_id, timeout, rate_limiter)

    if error:
        return PoolMetadataResult(pool_hash=pool_id, error=error)

    result = PoolMetadataResult(
        pool_hash=pool_id,
        pool_id=data.get("pool_id"),  # bech32 pool_id from Blockfrost
        ticker=data.get("ticker"),
        name=data.get("name"),
        description=data.get("description"),
        homepage=data.get("homepage"),
        metadata_url=data.get("url"),
        metadata_hash=data.get("hash"),
    )

    if result.ticker or result.name:
        result.success = True
    else:
        result.error = "no ticker or name in metadata"

    return result


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
    return _resolve_batch_generic(
        ids=drep_ids,
        resolve_fn=resolve_via_blockfrost,
        error_factory=lambda did, err: AnchorResult(anchor_url="", error=err),
        project_id=project_id,
        max_workers=max_workers,
        timeout=timeout,
        label="dreps",
    )


def resolve_pool_batch(
    pool_ids: list,
    project_id: str,
    max_workers: int = 10,
    timeout: int = 30,
) -> dict:
    """Resolve metadata for multiple pools via Blockfrost in parallel.

    Args:
        pool_ids: List of pool_id hex strings
        project_id: Blockfrost project_id for authentication
        max_workers: Thread pool size
        timeout: HTTP timeout per request

    Returns:
        {pool_id: PoolMetadataResult}
    """
    return _resolve_batch_generic(
        ids=pool_ids,
        resolve_fn=resolve_pool_metadata,
        error_factory=lambda pid, err: PoolMetadataResult(pool_hash=pid, error=err),
        project_id=project_id,
        max_workers=max_workers,
        timeout=timeout,
        label="pools",
    )


def _resolve_batch_generic(
    ids: list,
    resolve_fn: Callable,
    error_factory: Callable,
    project_id: str,
    max_workers: int,
    timeout: int,
    label: str,
) -> dict:
    """Generic batch resolver using Blockfrost with rate limiting."""
    results = {}
    limiter = _RateLimiter(rate=RATE_LIMIT_RPS, burst=500)

    logger.info("Resolving %d %s via Blockfrost (%d workers, %d req/s limit)",
                len(ids), label, max_workers, RATE_LIMIT_RPS)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                resolve_fn, item_id,
                project_id=project_id, timeout=timeout,
                rate_limiter=limiter,
            ): item_id
            for item_id in ids
        }

        for future in as_completed(futures):
            item_id = futures[future]
            try:
                results[item_id] = future.result()
            except Exception as e:
                logger.error("Unexpected error resolving %s: %s", item_id, e)
                results[item_id] = error_factory(item_id, str(e))

    return results

"""HTTP resolver for CIP-119 DRep anchor URLs."""

import ipaddress
import logging
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlparse

import requests

logger = logging.getLogger("yaci_s3.internal.anchor_resolver")

ALLOWED_SCHEMES = {"http", "https"}
MAX_RESPONSE_BYTES = 1 * 1024 * 1024  # 1 MB cap for anchor JSON


@dataclass
class AnchorResult:
    """Result of resolving a single anchor URL."""
    anchor_url: str
    drep_name: Optional[str] = None
    http_status: Optional[int] = None
    success: bool = False
    error: Optional[str] = None


def _rewrite_ipfs_url(url: str, ipfs_gateway: str) -> str:
    """Rewrite ipfs:// URLs to use an HTTP gateway."""
    if url.startswith("ipfs://"):
        cid = url[len("ipfs://"):]
        return f"{ipfs_gateway}{cid}"
    parsed = urlparse(url)
    if parsed.scheme == "ipfs":
        return f"{ipfs_gateway}{parsed.netloc}{parsed.path}"
    return url


def resolve_anchor(url: str, timeout: int = 10, ipfs_gateway: str = "https://ipfs.io/ipfs/") -> AnchorResult:
    """Fetch a CIP-119 anchor URL and extract givenName.

    Returns an AnchorResult with success=True if the URL resolved
    and contained a body.givenName field.
    """
    if not url or not url.strip():
        return AnchorResult(anchor_url=url or "", error="empty URL")

    resolved_url = _rewrite_ipfs_url(url.strip(), ipfs_gateway)

    # Validate scheme
    parsed = urlparse(resolved_url)
    if parsed.scheme not in ALLOWED_SCHEMES:
        return AnchorResult(anchor_url=url, error=f"blocked scheme: {parsed.scheme}")

    # Block private/reserved IPs (SSRF protection)
    try:
        hostname = parsed.hostname
        if hostname:
            addr_info = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
            for _, _, _, _, sockaddr in addr_info:
                ip = ipaddress.ip_address(sockaddr[0])
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    return AnchorResult(anchor_url=url, error=f"blocked private IP: {ip}")
    except (socket.gaierror, ValueError):
        pass  # DNS resolution failure will be caught by requests below

    try:
        resp = requests.get(resolved_url, timeout=timeout, stream=True, headers={
            "Accept": "application/json, application/ld+json, */*",
            "User-Agent": "yaci-s3/drep-profile",
        })
        result = AnchorResult(anchor_url=url, http_status=resp.status_code)

        if resp.status_code != 200:
            resp.close()
            result.error = f"HTTP {resp.status_code}"
            return result

        # Read with size cap to prevent OOM from large responses
        content = resp.raw.read(MAX_RESPONSE_BYTES + 1)
        resp.close()
        if len(content) > MAX_RESPONSE_BYTES:
            result.error = "response too large"
            return result

        try:
            import json
            data = json.loads(content)
        except (ValueError, UnicodeDecodeError):
            result.error = "invalid JSON"
            return result

        # CIP-119: givenName at top level or under body
        given_name = None
        if isinstance(data, dict):
            given_name = data.get("givenName")
            if not given_name and isinstance(data.get("body"), dict):
                given_name = data["body"].get("givenName")

        if given_name and isinstance(given_name, str) and given_name.strip():
            result.drep_name = given_name.strip()
            result.success = True
        else:
            result.error = "no givenName in JSON"

        return result

    except requests.exceptions.Timeout:
        return AnchorResult(anchor_url=url, error="timeout")
    except requests.exceptions.ConnectionError:
        return AnchorResult(anchor_url=url, error="connection error")
    except Exception as e:
        return AnchorResult(anchor_url=url, error=str(e))


def resolve_anchor_chain(
    urls: List[str],
    timeout: int = 10,
    ipfs_gateway: str = "https://ipfs.io/ipfs/",
) -> AnchorResult:
    """Try each URL in order, return first successful result.

    URLs should be ordered newest-first (highest block first).
    """
    for url in urls:
        result = resolve_anchor(url, timeout=timeout, ipfs_gateway=ipfs_gateway)
        if result.success:
            return result
    # Return last failure if none succeeded
    if urls:
        return result
    return AnchorResult(anchor_url="", error="no URLs to resolve")


def resolve_batch(
    drep_urls: dict,
    timeout: int = 10,
    ipfs_gateway: str = "https://ipfs.io/ipfs/",
    max_workers: int = 5,
) -> dict:
    """Resolve anchor URLs for multiple dreps in parallel.

    Args:
        drep_urls: {drep_id: [url1, url2, ...]} — URLs ordered newest-first
        timeout: HTTP timeout per request
        ipfs_gateway: IPFS gateway base URL
        max_workers: Thread pool size

    Returns:
        {drep_id: AnchorResult}
    """
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                resolve_anchor_chain, urls,
                timeout=timeout, ipfs_gateway=ipfs_gateway,
            ): drep_id
            for drep_id, urls in drep_urls.items()
        }
        for future in as_completed(futures):
            drep_id = futures[future]
            try:
                results[drep_id] = future.result()
            except Exception as e:
                logger.error("Unexpected error resolving %s: %s", drep_id, e)
                results[drep_id] = AnchorResult(anchor_url="", error=str(e))

    return results

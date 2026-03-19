"""Tests for Blockfrost-based DRep metadata resolver."""

import json
import time
from unittest.mock import patch, MagicMock

import requests

from yaci_s3.internal.anchor_resolver import (
    resolve_via_blockfrost,
    resolve_pool_metadata,
    resolve_batch,
    resolve_pool_batch,
    AnchorResult,
    PoolMetadataResult,
    _RateLimiter,
)

MOCK_PROJECT_ID = "test_project_id"
PATCH_GET = "yaci_s3.internal.anchor_resolver.requests.get"


def _mock_response(body_dict=None, status_code=200):
    """Helper to build a mock response."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = body_dict or {}
    return mock_resp


@patch(PATCH_GET)
def test_resolve_success_body_given_name(mock_get):
    mock_get.return_value = _mock_response({
        "url": "https://example.com/meta.json",
        "json_metadata": {"body": {"givenName": "Alice DRep"}},
    })

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=5)
    assert result.success
    assert result.drep_name == "Alice DRep"
    assert result.anchor_url == "https://example.com/meta.json"
    assert result.http_status == 200


@patch(PATCH_GET)
def test_resolve_success_top_level_given_name(mock_get):
    mock_get.return_value = _mock_response({
        "url": "https://example.com/meta.json",
        "json_metadata": {"givenName": "Bob DRep"},
    })

    result = resolve_via_blockfrost("drep2def", MOCK_PROJECT_ID, timeout=5)
    assert result.success
    assert result.drep_name == "Bob DRep"


@patch(PATCH_GET)
def test_resolve_no_given_name(mock_get):
    mock_get.return_value = _mock_response({
        "url": "https://example.com/meta.json",
        "json_metadata": {"body": {"otherField": "value"}},
    })

    result = resolve_via_blockfrost("drep3ghi", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert result.http_status == 200
    assert "no givenName" in result.error


@patch(PATCH_GET)
def test_resolve_404_no_metadata(mock_get):
    mock_get.return_value = _mock_response(status_code=404)

    result = resolve_via_blockfrost("drep_unknown", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert result.http_status == 404
    assert result.error == "no metadata"


@patch("yaci_s3.internal.anchor_resolver.time.sleep")
@patch(PATCH_GET)
def test_resolve_429_retries_then_succeeds(mock_get, mock_sleep):
    """429 should retry with backoff and succeed if subsequent attempt works."""
    mock_get.side_effect = [
        _mock_response(status_code=429),
        _mock_response({
            "url": "https://example.com/meta.json",
            "json_metadata": {"body": {"givenName": "Alice DRep"}},
        }),
    ]

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=5)
    assert result.success
    assert result.drep_name == "Alice DRep"
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(2)  # 2^1 backoff


@patch("yaci_s3.internal.anchor_resolver.time.sleep")
@patch(PATCH_GET)
def test_resolve_429_exhausts_retries(mock_get, mock_sleep):
    """429 on all attempts should return rate limited error."""
    mock_get.return_value = _mock_response(status_code=429)

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert result.error == "rate limited"
    assert mock_get.call_count == 3  # MAX_RETRIES = 3


@patch(PATCH_GET)
def test_resolve_500_server_error(mock_get):
    mock_get.return_value = _mock_response(status_code=500)

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert result.http_status == 500
    assert "HTTP 500" in result.error


@patch(PATCH_GET)
def test_resolve_timeout(mock_get):
    mock_get.side_effect = requests.exceptions.Timeout("timed out")

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=1)
    assert not result.success
    assert result.error == "timeout"


@patch(PATCH_GET)
def test_resolve_connection_error(mock_get):
    mock_get.side_effect = requests.exceptions.ConnectionError("refused")

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=1)
    assert not result.success
    assert result.error == "connection error"


@patch(PATCH_GET)
def test_resolve_no_json_metadata(mock_get):
    """Response with no json_metadata field."""
    mock_get.return_value = _mock_response({
        "url": "https://example.com/meta.json",
    })

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert "no givenName" in result.error


def test_rate_limiter_burst():
    """Rate limiter should allow burst requests immediately."""
    limiter = _RateLimiter(rate=10, burst=5)
    start = time.monotonic()
    for _ in range(5):
        limiter.acquire()
    elapsed = time.monotonic() - start
    # 5 burst requests should complete nearly instantly (< 0.5s)
    assert elapsed < 0.5


@patch("yaci_s3.internal.anchor_resolver.resolve_via_blockfrost")
def test_resolve_batch(mock_resolve):
    mock_resolve.side_effect = [
        AnchorResult(anchor_url="https://a.com/meta.json", drep_name="Alice", success=True, http_status=200),
        AnchorResult(anchor_url="https://b.com/meta.json", drep_name="Bob", success=True, http_status=200),
    ]

    results = resolve_batch(["drep1abc", "drep2def"], project_id=MOCK_PROJECT_ID, max_workers=2)
    assert len(results) == 2
    assert results["drep1abc"].drep_name == "Alice"
    assert results["drep2def"].drep_name == "Bob"


# --- Pool metadata tests ---

@patch(PATCH_GET)
def test_resolve_pool_metadata_success(mock_get):
    mock_get.return_value = _mock_response({
        "pool_id": "pool1abc",
        "hex": "d2f12c2f3094ed07",
        "url": "https://example.com/poolmeta.json",
        "hash": "abc123",
        "ticker": "TAPSY",
        "name": "TapTap Vienna",
        "description": "low fees",
        "homepage": "https://tap-ada.github.io",
    })

    result = resolve_pool_metadata("d2f12c2f3094ed07", MOCK_PROJECT_ID, timeout=5)
    assert result.success
    assert result.pool_hash == "d2f12c2f3094ed07"
    assert result.pool_id == "pool1abc"
    assert result.ticker == "TAPSY"
    assert result.name == "TapTap Vienna"
    assert result.description == "low fees"
    assert result.homepage == "https://tap-ada.github.io"
    assert result.metadata_url == "https://example.com/poolmeta.json"
    assert result.http_status == 200


@patch(PATCH_GET)
def test_resolve_pool_metadata_404(mock_get):
    mock_get.return_value = _mock_response(status_code=404)

    result = resolve_pool_metadata("deadbeef", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert result.http_status == 404
    assert result.error == "no metadata"


@patch(PATCH_GET)
def test_resolve_pool_metadata_no_ticker_no_name(mock_get):
    mock_get.return_value = _mock_response({
        "pool_id": "pool1abc",
        "hex": "d2f12c2f3094ed07",
        "url": "https://example.com/poolmeta.json",
        "hash": "abc123",
        "ticker": None,
        "name": None,
        "description": None,
        "homepage": None,
    })

    result = resolve_pool_metadata("d2f12c2f3094ed07", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert "no ticker or name" in result.error


@patch("yaci_s3.internal.anchor_resolver.resolve_pool_metadata")
def test_resolve_pool_batch(mock_resolve):
    mock_resolve.side_effect = [
        PoolMetadataResult(pool_hash="hash1", pool_id="pool1", ticker="AAA", name="Pool A", success=True, http_status=200),
        PoolMetadataResult(pool_hash="hash2", pool_id="pool2", ticker="BBB", name="Pool B", success=True, http_status=200),
    ]

    results = resolve_pool_batch(["hash1", "hash2"], project_id=MOCK_PROJECT_ID, max_workers=2)
    assert len(results) == 2
    assert results["hash1"].ticker == "AAA"
    assert results["hash1"].pool_id == "pool1"
    assert results["hash2"].name == "Pool B"

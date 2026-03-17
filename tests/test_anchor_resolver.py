"""Tests for Blockfrost-based DRep metadata resolver."""

import json
from unittest.mock import patch, MagicMock

import requests

from yaci_s3.internal.anchor_resolver import (
    resolve_via_blockfrost,
    resolve_batch,
    AnchorResult,
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


@patch(PATCH_GET)
def test_resolve_429_rate_limited(mock_get):
    mock_get.return_value = _mock_response(status_code=429)

    result = resolve_via_blockfrost("drep1abc", MOCK_PROJECT_ID, timeout=5)
    assert not result.success
    assert result.http_status == 429
    assert result.error == "rate limited"


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

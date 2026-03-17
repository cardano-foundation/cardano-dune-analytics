"""Tests for CIP-119 anchor URL resolver."""

import json
import socket
from unittest.mock import patch, MagicMock

from yaci_s3.internal.anchor_resolver import (
    _rewrite_ipfs_url,
    resolve_anchor,
    resolve_anchor_chain,
    resolve_batch,
    AnchorResult,
)


def test_rewrite_ipfs_url():
    gateway = "https://ipfs.io/ipfs/"
    assert _rewrite_ipfs_url("ipfs://QmABC123", gateway) == "https://ipfs.io/ipfs/QmABC123"
    assert _rewrite_ipfs_url("https://example.com/meta.json", gateway) == "https://example.com/meta.json"
    assert _rewrite_ipfs_url("ipfs://QmABC123/path", gateway) == "https://ipfs.io/ipfs/QmABC123/path"


def test_rewrite_ipfs_url_custom_gateway():
    gateway = "https://cf-ipfs.com/ipfs/"
    assert _rewrite_ipfs_url("ipfs://QmXYZ", gateway) == "https://cf-ipfs.com/ipfs/QmXYZ"


def test_resolve_anchor_empty_url():
    result = resolve_anchor("", timeout=1)
    assert not result.success
    assert result.error == "empty URL"


def test_resolve_anchor_none_url():
    result = resolve_anchor(None, timeout=1)
    assert not result.success
    assert result.error == "empty URL"


def test_resolve_anchor_blocked_scheme():
    result = resolve_anchor("file:///etc/passwd", timeout=1)
    assert not result.success
    assert "blocked scheme" in result.error


def test_resolve_anchor_blocked_private_ip():
    with patch("yaci_s3.internal.anchor_resolver.socket.getaddrinfo") as mock_dns:
        mock_dns.return_value = [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 0))]
        result = resolve_anchor("https://example.com/drep.json", timeout=1)
        assert not result.success
        assert "blocked private IP" in result.error


def _mock_response(body_dict=None, status_code=200, body_bytes=None):
    """Helper to build a mock streaming response."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    if body_bytes is None and body_dict is not None:
        body_bytes = json.dumps(body_dict).encode()
    mock_resp.raw.read.return_value = body_bytes or b""
    mock_resp.close = MagicMock()
    return mock_resp


# Patch both requests.get and socket.getaddrinfo to bypass SSRF check
_PATCHES = [
    "yaci_s3.internal.anchor_resolver.requests.get",
    "yaci_s3.internal.anchor_resolver.socket.getaddrinfo",
]


def _public_dns(*args, **kwargs):
    """Fake getaddrinfo returning a public IP."""
    return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("93.184.216.34", 0))]


@patch(_PATCHES[1], side_effect=_public_dns)
@patch(_PATCHES[0])
def test_resolve_anchor_success(mock_get, mock_dns):
    mock_get.return_value = _mock_response({"body": {"givenName": "Alice DRep"}})

    result = resolve_anchor("https://example.com/drep.json", timeout=5)
    assert result.success
    assert result.drep_name == "Alice DRep"
    assert result.http_status == 200


@patch(_PATCHES[1], side_effect=_public_dns)
@patch(_PATCHES[0])
def test_resolve_anchor_top_level_given_name(mock_get, mock_dns):
    mock_get.return_value = _mock_response({"givenName": "Bob DRep"})

    result = resolve_anchor("https://example.com/drep.json", timeout=5)
    assert result.success
    assert result.drep_name == "Bob DRep"


@patch(_PATCHES[1], side_effect=_public_dns)
@patch(_PATCHES[0])
def test_resolve_anchor_no_given_name(mock_get, mock_dns):
    mock_get.return_value = _mock_response({"body": {"otherField": "value"}})

    result = resolve_anchor("https://example.com/drep.json", timeout=5)
    assert not result.success
    assert result.http_status == 200
    assert "no givenName" in result.error


@patch(_PATCHES[1], side_effect=_public_dns)
@patch(_PATCHES[0])
def test_resolve_anchor_http_404(mock_get, mock_dns):
    mock_get.return_value = _mock_response(status_code=404)

    result = resolve_anchor("https://example.com/missing.json", timeout=5)
    assert not result.success
    assert result.http_status == 404
    assert "HTTP 404" in result.error


@patch(_PATCHES[1], side_effect=_public_dns)
@patch(_PATCHES[0])
def test_resolve_anchor_invalid_json(mock_get, mock_dns):
    mock_get.return_value = _mock_response(body_bytes=b"not json at all")

    result = resolve_anchor("https://example.com/bad.json", timeout=5)
    assert not result.success
    assert "invalid JSON" in result.error


@patch(_PATCHES[1], side_effect=_public_dns)
@patch(_PATCHES[0])
def test_resolve_anchor_response_too_large(mock_get, mock_dns):
    from yaci_s3.internal.anchor_resolver import MAX_RESPONSE_BYTES
    mock_get.return_value = _mock_response(body_bytes=b"x" * (MAX_RESPONSE_BYTES + 2))

    result = resolve_anchor("https://example.com/huge.json", timeout=5)
    assert not result.success
    assert "response too large" in result.error


@patch("yaci_s3.internal.anchor_resolver.requests.get")
def test_resolve_anchor_timeout(mock_get):
    import requests
    mock_get.side_effect = requests.exceptions.Timeout("timed out")

    result = resolve_anchor("https://slow.example.com/drep.json", timeout=1)
    assert not result.success
    assert result.error == "timeout"


@patch("yaci_s3.internal.anchor_resolver.requests.get")
def test_resolve_anchor_connection_error(mock_get):
    import requests
    mock_get.side_effect = requests.exceptions.ConnectionError("refused")

    result = resolve_anchor("https://down.example.com/drep.json", timeout=1)
    assert not result.success
    assert result.error == "connection error"


@patch("yaci_s3.internal.anchor_resolver.resolve_anchor")
def test_resolve_anchor_chain_first_succeeds(mock_resolve):
    mock_resolve.return_value = AnchorResult(
        anchor_url="https://a.com/meta.json", drep_name="Alice", success=True, http_status=200,
    )
    result = resolve_anchor_chain(["https://a.com/meta.json", "https://b.com/meta.json"])
    assert result.success
    assert result.drep_name == "Alice"
    mock_resolve.assert_called_once()


@patch("yaci_s3.internal.anchor_resolver.resolve_anchor")
def test_resolve_anchor_chain_fallback(mock_resolve):
    fail = AnchorResult(anchor_url="https://a.com/meta.json", success=False, error="HTTP 404", http_status=404)
    ok = AnchorResult(anchor_url="https://b.com/meta.json", drep_name="Bob", success=True, http_status=200)
    mock_resolve.side_effect = [fail, ok]

    result = resolve_anchor_chain(["https://a.com/meta.json", "https://b.com/meta.json"])
    assert result.success
    assert result.drep_name == "Bob"
    assert mock_resolve.call_count == 2


@patch("yaci_s3.internal.anchor_resolver.resolve_anchor")
def test_resolve_anchor_chain_all_fail(mock_resolve):
    fail = AnchorResult(anchor_url="https://a.com/meta.json", success=False, error="HTTP 500", http_status=500)
    mock_resolve.return_value = fail

    result = resolve_anchor_chain(["https://a.com/meta.json", "https://b.com/meta.json"])
    assert not result.success


def test_resolve_anchor_chain_empty_list():
    result = resolve_anchor_chain([])
    assert not result.success
    assert result.error == "no URLs to resolve"


@patch("yaci_s3.internal.anchor_resolver.resolve_anchor_chain")
def test_resolve_batch(mock_chain):
    mock_chain.side_effect = [
        AnchorResult(anchor_url="https://a.com/meta.json", drep_name="Alice", success=True),
        AnchorResult(anchor_url="https://b.com/meta.json", drep_name="Bob", success=True),
    ]

    drep_urls = {
        "drep1abc": ["https://a.com/meta.json"],
        "drep2def": ["https://b.com/meta.json"],
    }
    results = resolve_batch(drep_urls, max_workers=2)
    assert len(results) == 2
    assert results["drep1abc"].drep_name == "Alice"
    assert results["drep2def"].drep_name == "Bob"

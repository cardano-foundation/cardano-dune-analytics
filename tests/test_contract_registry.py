"""Tests for contract_registry exporter — changed-files-only logic."""

import json
import tempfile
from unittest.mock import MagicMock, patch

from yaci_s3.config import AppConfig
from yaci_s3.db import TrackingDB
from yaci_s3.external.contract_registry import (
    ContractRegistryExporter,
    GitHubClient,
    parse_stricahq_project,
    parse_crfa_v2_project,
    parse_crfa_v1_project,
)


def _make_config(base_path):
    return AppConfig(
        pg_host="", pg_port=5432, pg_db="", pg_user="", pg_password="",
        pg_schema="public", s3_bucket="test", base_data_path=base_path,
        sqlite_path=":memory:", blockfrost_project_id="test_project_id",
        anchor_max_workers=2,
    )


def _stricahq_json(project_name, contracts):
    """Build a StricaHQ-format JSON string."""
    return json.dumps({
        "projectName": project_name,
        "category": "DeFi",
        "subCategory": "",
        "contracts": [
            {"scriptHash": h, "name": n, "language": "Plutus", "purpose": "spend"}
            for h, n in contracts
        ],
    })


def _crfa_v2_json(project_name, scripts):
    """Build a CRFA v2-format JSON string."""
    return json.dumps({
        "dAppName": project_name,
        "category": "DEX",
        "subCategory": "",
        "scripts": [
            {"scriptHash": h, "name": n, "type": "Plutus", "purpose": "spend"}
            for h, n in scripts
        ],
    })


# ---------------------------------------------------------------------------
# GitHubClient.get_changed_files
# ---------------------------------------------------------------------------

class TestGetChangedFiles:

    def test_returns_filtered_json_files(self):
        client = GitHubClient()
        compare_resp = {
            "files": [
                {"filename": "projects/foo.json", "status": "modified"},
                {"filename": "projects/bar.json", "status": "added"},
                {"filename": "projects/readme.md", "status": "modified"},
                {"filename": "other/baz.json", "status": "modified"},
                {"filename": "projects/removed.json", "status": "removed"},
            ]
        }
        with patch.object(client, "_get", return_value=compare_resp):
            result = client.get_changed_files("owner", "repo", "aaa", "bbb", "projects/")

        assert result == ["projects/foo.json", "projects/bar.json"]

    def test_returns_none_on_api_failure(self):
        client = GitHubClient()
        with patch.object(client, "_get", return_value=None):
            result = client.get_changed_files("owner", "repo", "aaa", "bbb", "projects/")
        assert result is None

    def test_returns_empty_when_no_json_changed(self):
        client = GitHubClient()
        compare_resp = {"files": [{"filename": "projects/readme.md", "status": "modified"}]}
        with patch.object(client, "_get", return_value=compare_resp):
            result = client.get_changed_files("owner", "repo", "aaa", "bbb", "projects/")
        assert result == []


# ---------------------------------------------------------------------------
# fetch_data: changed-files-only path
# ---------------------------------------------------------------------------

class TestFetchDataChangedFiles:

    def _make_exporter(self, base_path):
        config = _make_config(base_path)
        db = TrackingDB(config.sqlite_path)
        uploader = MagicMock()
        exporter = ContractRegistryExporter(config, db, uploader)
        exporter.client = MagicMock(spec=GitHubClient)
        return exporter

    def test_first_run_fetches_all_files(self):
        """With no stored SHA, should use get_tree (full fetch), not get_changed_files."""
        with tempfile.TemporaryDirectory() as base:
            exporter = self._make_exporter(base)

            # Simulate: latest SHA is "new_sha", no stored SHA
            exporter.client.get_latest_commit_sha.return_value = "new_sha"

            # get_tree returns one file
            exporter.client.get_tree.return_value = [
                {"path": "projects/foo.json", "type": "blob"},
            ]
            exporter.client.get_file_content.return_value = _stricahq_json(
                "FooProject", [("hash_a", "ContractA")]
            )

            table = exporter.fetch_data()

            # Should have called get_tree (full), NOT get_changed_files
            assert exporter.client.get_tree.call_count >= 1
            exporter.client.get_changed_files.assert_not_called()

            assert table is not None
            assert table.num_rows >= 1

    def test_subsequent_run_uses_changed_files(self):
        """With stored SHA, should use get_changed_files (incremental)."""
        with tempfile.TemporaryDirectory() as base:
            exporter = self._make_exporter(base)

            # Seed a stored SHA for stricahq
            exporter.db.update_contract_registry_state("stricahq", "old_sha")
            # Seed SHAs for the other sources so they appear unchanged
            exporter.db.update_contract_registry_state("crfa_v2", "unchanged_sha")
            exporter.db.update_contract_registry_state("crfa_v1", "unchanged_sha")

            # stricahq has new commit; others unchanged
            def latest_sha(owner, repo, branch):
                if owner == "StricaHQ":
                    return "new_sha"
                return "unchanged_sha"

            exporter.client.get_latest_commit_sha.side_effect = latest_sha

            # Changed files returns one file
            exporter.client.get_changed_files.return_value = ["projects/foo.json"]
            exporter.client.get_file_content.return_value = _stricahq_json(
                "FooProject", [("hash_a", "ContractA")]
            )

            table = exporter.fetch_data()

            # Should have called get_changed_files, NOT get_tree
            exporter.client.get_changed_files.assert_called_once_with(
                "StricaHQ", "cardano-contracts-registry", "old_sha", "new_sha", "projects/",
            )
            exporter.client.get_tree.assert_not_called()

            assert table is not None
            assert table.num_rows == 1
            assert table.column("script_hash").to_pylist() == ["hash_a"]

    def test_compare_api_failure_falls_back_to_full_fetch(self):
        """If get_changed_files returns None, should fall back to full fetch."""
        with tempfile.TemporaryDirectory() as base:
            exporter = self._make_exporter(base)

            exporter.db.update_contract_registry_state("stricahq", "old_sha")
            exporter.db.update_contract_registry_state("crfa_v2", "unchanged_sha")
            exporter.db.update_contract_registry_state("crfa_v1", "unchanged_sha")

            def latest_sha(owner, repo, branch):
                if owner == "StricaHQ":
                    return "new_sha"
                return "unchanged_sha"

            exporter.client.get_latest_commit_sha.side_effect = latest_sha

            # Compare API fails
            exporter.client.get_changed_files.return_value = None
            # Fall back: get_tree returns files
            exporter.client.get_tree.return_value = [
                {"path": "projects/foo.json", "type": "blob"},
            ]
            exporter.client.get_file_content.return_value = _stricahq_json(
                "FooProject", [("hash_a", "ContractA")]
            )

            table = exporter.fetch_data()

            # Both called: changed_files tried first, then get_tree as fallback
            exporter.client.get_changed_files.assert_called_once()
            exporter.client.get_tree.assert_called_once()
            assert table is not None
            assert table.num_rows == 1

    def test_no_changed_json_files_returns_none(self):
        """If changed files list is empty, should return None (no export)."""
        with tempfile.TemporaryDirectory() as base:
            exporter = self._make_exporter(base)

            exporter.db.update_contract_registry_state("stricahq", "old_sha")
            exporter.db.update_contract_registry_state("crfa_v2", "unchanged_sha")
            exporter.db.update_contract_registry_state("crfa_v1", "unchanged_sha")

            def latest_sha(owner, repo, branch):
                if owner == "StricaHQ":
                    return "new_sha"
                return "unchanged_sha"

            exporter.client.get_latest_commit_sha.side_effect = latest_sha
            exporter.client.get_changed_files.return_value = []

            table = exporter.fetch_data()

            # No records to export, SHA still updated
            assert table is None

    def test_insert_or_ignore_preserves_first_seen_at(self):
        """Re-exporting a hash should not overwrite its first_seen_at."""
        with tempfile.TemporaryDirectory() as base:
            exporter = self._make_exporter(base)

            # Pre-seed a hash as already known
            exporter.db.conn.execute(
                "INSERT INTO contract_registry_hashes (script_hash, source, first_seen_at) VALUES (?, ?, ?)",
                ("hash_a", "stricahq", "2024-01-01T00:00:00+00:00"),
            )
            exporter.db.conn.commit()

            exporter.db.update_contract_registry_state("stricahq", "old_sha")
            exporter.db.update_contract_registry_state("crfa_v2", "unchanged_sha")
            exporter.db.update_contract_registry_state("crfa_v1", "unchanged_sha")

            def latest_sha(owner, repo, branch):
                if owner == "StricaHQ":
                    return "new_sha"
                return "unchanged_sha"

            exporter.client.get_latest_commit_sha.side_effect = latest_sha
            exporter.client.get_changed_files.return_value = ["projects/foo.json"]
            exporter.client.get_file_content.return_value = _stricahq_json(
                "UpdatedProject", [("hash_a", "ContractA")]
            )

            table = exporter.fetch_data()

            # The exported table has the updated project name
            assert table is not None
            assert table.column("project_name").to_pylist() == ["UpdatedProject"]

            # But first_seen_at in DB is preserved (not overwritten)
            cursor = exporter.db.conn.execute(
                "SELECT first_seen_at FROM contract_registry_hashes WHERE script_hash = ?",
                ("hash_a",),
            )
            row = cursor.fetchone()
            assert row["first_seen_at"] == "2024-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Parser unit tests
# ---------------------------------------------------------------------------

class TestParsers:

    def test_parse_stricahq(self):
        content = _stricahq_json("TestProj", [("abc123", "MainContract")])
        records = parse_stricahq_project(content, "projects/test.json")
        assert len(records) == 1
        assert records[0]["script_hash"] == "abc123"
        assert records[0]["source"] == "stricahq"

    def test_parse_crfa_v2(self):
        content = _crfa_v2_json("TestDex", [("def456", "SwapContract")])
        records = parse_crfa_v2_project(content, "dApps_v2/test.json")
        assert len(records) == 1
        assert records[0]["script_hash"] == "def456"
        assert records[0]["source"] == "crfa_v2"

    def test_parse_crfa_v1(self):
        content = json.dumps({
            "dAppName": "OldDex",
            "category": "DEX",
            "subCategory": "",
            "scripts": [{
                "name": "OldSwap",
                "purpose": "spend",
                "type": "Plutus",
                "versions": [
                    {"scriptHash": "ghi789"},
                    {"scriptHash": "jkl012"},
                ],
            }],
        })
        records = parse_crfa_v1_project(content, "dApps/test.json")
        assert len(records) == 2
        assert records[0]["script_hash"] == "ghi789"
        assert records[1]["script_hash"] == "jkl012"

    def test_invalid_json_returns_empty(self):
        assert parse_stricahq_project("not json", "test.json") == []
        assert parse_crfa_v2_project("{bad", "test.json") == []
        assert parse_crfa_v1_project("", "test.json") == []

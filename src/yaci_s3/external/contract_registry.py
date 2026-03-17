"""Contract registry exporter - fetches script hash data from GitHub repos."""

import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

import pyarrow as pa
import requests

from . import register
from .base import ExternalExporter
from ..config import AppConfig
from ..db import TrackingDB
from ..uploader import S3Uploader

logger = logging.getLogger("yaci_s3.external.contract_registry")

# Sources in priority order (highest first)
SOURCES = [
    {
        "name": "stricahq",
        "owner": "StricaHQ",
        "repo": "cardano-contracts-registry",
        "branch": "master",
        "path": "projects/",
        "priority": 1,
    },
    {
        "name": "crfa_v2",
        "owner": "mezuny",
        "repo": "crfa-offchain-data-registry",
        "branch": "main",
        "path": "dApps_v2/",
        "priority": 2,
    },
    {
        "name": "crfa_v1",
        "owner": "mezuny",
        "repo": "crfa-offchain-data-registry",
        "branch": "main",
        "path": "dApps/",
        "priority": 3,
    },
]

# Map source names to priority for quick lookup
SOURCE_PRIORITY = {s["name"]: s["priority"] for s in SOURCES}


class GitHubClient:
    """Client for fetching contract registry data from GitHub."""

    def __init__(self, token: str = ""):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/vnd.github.v3+json"})
        if token:
            self.session.headers["Authorization"] = f"token {token}"

    def get_latest_commit_sha(self, owner: str, repo: str, branch: str) -> Optional[str]:
        """Get the latest commit SHA for a branch."""
        url = f"https://api.github.com/repos/{owner}/{repo}/commits/{branch}"
        resp = self._get(url)
        if resp is None:
            return None
        return resp.get("sha")

    def get_tree(self, owner: str, repo: str, branch: str, path: str) -> List[dict]:
        """Get the file tree for a path in a repo using the Git Trees API (recursive)."""
        # First get the tree SHA for the branch
        url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
        resp = self._get(url)
        if resp is None:
            return []

        # Filter to files under the specified path
        prefix = path.rstrip("/") + "/"
        files = []
        for item in resp.get("tree", []):
            if item["type"] == "blob" and item["path"].startswith(prefix):
                files.append(item)
        return files

    def get_file_content(self, owner: str, repo: str, branch: str, path: str) -> Optional[str]:
        """Fetch raw file content from GitHub."""
        url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

    def _get(self, url: str) -> Optional[dict]:
        """GET a GitHub API endpoint with basic error handling."""
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 403:
                # Rate limited
                reset_at = resp.headers.get("X-RateLimit-Reset")
                logger.warning("GitHub rate limit hit. Reset at: %s", reset_at)
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("GitHub API request failed: %s", e)
            return None


def parse_stricahq_project(content: str, file_path: str) -> List[dict]:
    """Parse a StricaHQ project JSON file into contract records.

    StricaHQ format: top-level object with projectName/labelPrefix, category,
    and a 'contracts' array where each entry has scriptHash, name, language, etc.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("Failed to parse StricaHQ file: %s", file_path)
        return []

    project_name = data.get("projectName", "") or data.get("labelPrefix", "")
    category = data.get("category", "")
    sub_category = data.get("subCategory", "")

    records = []
    for contract in data.get("contracts", []):
        script_hash = contract.get("scriptHash", "").strip()
        if not script_hash:
            continue
        records.append({
            "script_hash": script_hash,
            "project_name": project_name,
            "contract_name": contract.get("name", ""),
            "category": category,
            "sub_category": sub_category,
            "purpose": contract.get("purpose", ""),
            "language": contract.get("language", ""),
            "source": "stricahq",
        })

    return records


def parse_crfa_v2_project(content: str, file_path: str) -> List[dict]:
    """Parse a CRFA v2 dApp JSON file into contract records.

    CRFA v2 format: flat scripts array with scriptHash, name, purpose, type directly.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("Failed to parse CRFA v2 file: %s", file_path)
        return []

    project_name = data.get("dAppName", "") or data.get("projectName", "")
    category = data.get("category", "")
    sub_category = data.get("subCategory", "")

    records = []
    for script in data.get("scripts", []):
        script_hash = script.get("scriptHash", "").strip()
        if not script_hash:
            continue
        records.append({
            "script_hash": script_hash,
            "project_name": project_name,
            "contract_name": script.get("name", ""),
            "category": category,
            "sub_category": sub_category,
            "purpose": script.get("purpose", ""),
            "language": script.get("type", ""),
            "source": "crfa_v2",
        })

    return records


def parse_crfa_v1_project(content: str, file_path: str) -> List[dict]:
    """Parse a CRFA v1 dApp JSON file into contract records.

    CRFA v1 format: scripts have nested 'versions' array, each version has
    its own scriptHash. Purpose and type are on the script, not the version.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        logger.warning("Failed to parse CRFA v1 file: %s", file_path)
        return []

    project_name = data.get("dAppName", "") or data.get("projectName", "")
    category = data.get("category", "")
    sub_category = data.get("subCategory", "")

    records = []
    for script in data.get("scripts", []):
        purpose = script.get("purpose", "")
        language = script.get("type", "")

        for version in script.get("versions", []):
            script_hash = version.get("scriptHash", "").strip()
            if not script_hash:
                continue
            records.append({
                "script_hash": script_hash,
                "project_name": project_name,
                "contract_name": script.get("name", "") or script.get("id", ""),
                "category": category,
                "sub_category": sub_category,
                "purpose": purpose,
                "language": language,
                "source": "crfa_v1",
            })

    return records


# Map source names to their parser functions
PARSERS = {
    "stricahq": parse_stricahq_project,
    "crfa_v2": parse_crfa_v2_project,
    "crfa_v1": parse_crfa_v1_project,
}


@register("contract_registry")
class ContractRegistryExporter(ExternalExporter):
    """Exports contract registry data from GitHub repos (incremental)."""

    name = "contract_registry"

    def __init__(self, config: AppConfig, db: TrackingDB, uploader: S3Uploader):
        super().__init__(config, db, uploader)
        self.client = GitHubClient(token=config.github_token)

    def fetch_data(self) -> Optional[pa.Table]:
        """Fetch new contract registry entries from GitHub.

        Checks each source for new commits, parses script hashes,
        and returns only truly new hashes.
        """
        # Step 1: Check for new commits
        sources_to_update = []
        for source in SOURCES:
            latest_sha = self.client.get_latest_commit_sha(
                source["owner"], source["repo"], source["branch"],
            )
            if latest_sha is None:
                logger.warning("Could not fetch commit SHA for %s, skipping", source["name"])
                continue

            stored_sha = self.db.get_contract_registry_state(source["name"])
            if stored_sha == latest_sha:
                logger.info("[%s] No new commits (SHA: %s)", source["name"], latest_sha[:8])
                continue

            logger.info(
                "[%s] New commits detected: %s -> %s",
                source["name"],
                stored_sha[:8] if stored_sha else "none",
                latest_sha[:8],
            )
            sources_to_update.append((source, latest_sha))

        if not sources_to_update:
            logger.info("No sources have new commits, nothing to export")
            return None

        # Step 2: Fetch and parse all records from updated sources
        all_records = []
        for source, latest_sha in sources_to_update:
            records = self._fetch_source_records(source)
            all_records.extend(records)
            logger.info("[%s] Parsed %d records", source["name"], len(records))

        if not all_records:
            # Update SHAs even if no records found (source might just have metadata changes)
            for source, latest_sha in sources_to_update:
                self.db.update_contract_registry_state(source["name"], latest_sha)
            return None

        # Step 3: Filter to truly new hashes, respecting priority
        known_hashes = self.db.get_known_script_hashes()
        hash_sources = self.db.get_script_hash_sources()

        new_records = []
        seen_in_batch = {}  # script_hash -> record (keep highest priority)

        for record in all_records:
            sh = record["script_hash"]
            source_name = record["source"]
            priority = SOURCE_PRIORITY[source_name]

            # Skip if already known from same or higher priority source
            if sh in known_hashes:
                existing_source = hash_sources.get(sh)
                if existing_source and SOURCE_PRIORITY.get(existing_source, 99) <= priority:
                    continue

            # Within this batch, keep highest priority
            if sh in seen_in_batch:
                existing_priority = SOURCE_PRIORITY[seen_in_batch[sh]["source"]]
                if existing_priority <= priority:
                    continue

            seen_in_batch[sh] = record

        new_records = list(seen_in_batch.values())

        if not new_records:
            logger.info("No new script hashes found")
            # Still update SHAs
            for source, latest_sha in sources_to_update:
                self.db.update_contract_registry_state(source["name"], latest_sha)
            return None

        logger.info("Found %d new script hashes", len(new_records))

        # Step 4: Build the Arrow table
        now = datetime.now(timezone.utc).isoformat()
        table = self._records_to_table(new_records, now)

        # Step 5: Update tracking state (SHAs and hashes) atomically
        # Only update after successful table creation
        try:
            now_ts = datetime.now(timezone.utc).isoformat()
            for r in new_records:
                self.db.conn.execute(
                    "INSERT OR IGNORE INTO contract_registry_hashes (script_hash, source, first_seen_at) VALUES (?, ?, ?)",
                    (r["script_hash"], r["source"], now_ts),
                )
            for source, latest_sha in sources_to_update:
                self.db.conn.execute(
                    "INSERT OR REPLACE INTO contract_registry_state (source, last_commit_sha, last_checked_at) VALUES (?, ?, ?)",
                    (source["name"], latest_sha, now_ts),
                )
            self.db.conn.commit()
        except Exception:
            self.db.conn.rollback()
            raise

        return table

    def _fetch_source_records(self, source: dict) -> List[dict]:
        """Fetch all records from a single source."""
        files = self.client.get_tree(
            source["owner"], source["repo"], source["branch"], source["path"],
        )

        parser = PARSERS[source["name"]]
        records = []

        json_files = [f for f in files if f["path"].endswith(".json")]
        logger.info("[%s] Found %d JSON files", source["name"], len(json_files))

        for file_info in json_files:
            content = self.client.get_file_content(
                source["owner"], source["repo"], source["branch"], file_info["path"],
            )
            if content is None:
                continue

            file_records = parser(content, file_info["path"])
            records.extend(file_records)

        return records

    def _records_to_table(self, records: List[dict], now: str) -> pa.Table:
        """Convert record dicts to a PyArrow Table."""
        schema = pa.schema([
            ("script_hash", pa.string()),
            ("project_name", pa.string()),
            ("contract_name", pa.string()),
            ("category", pa.string()),
            ("sub_category", pa.string()),
            ("purpose", pa.string()),
            ("language", pa.string()),
            ("insert_datetime", pa.string()),
            ("update_datetime", pa.string()),
        ])

        arrays = {
            "script_hash": pa.array([r["script_hash"] for r in records]),
            "project_name": pa.array([r["project_name"] for r in records]),
            "contract_name": pa.array([r["contract_name"] for r in records]),
            "category": pa.array([r["category"] for r in records]),
            "sub_category": pa.array([r["sub_category"] for r in records]),
            "purpose": pa.array([r["purpose"] for r in records]),
            "language": pa.array([r["language"] for r in records]),
            "insert_datetime": pa.array([now] * len(records)),
            "update_datetime": pa.array([now] * len(records)),
        }

        return pa.table(arrays, schema=schema)

    def validate(self, table: pa.Table) -> bool:
        """Validate contract registry data."""
        if len(table) == 0:
            logger.error("Validation failed: empty table")
            return False

        # Check script_hash uniqueness
        hashes = table.column("script_hash").to_pylist()
        if len(set(hashes)) != len(hashes):
            logger.error("Validation failed: duplicate script hashes in export")
            return False

        logger.info("Validation passed: %d unique script hashes", len(hashes))
        return True

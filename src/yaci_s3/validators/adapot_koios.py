"""Validate adapot parquet data against Koios /totals API."""

import logging
import time
from glob import glob as pyglob
from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import requests

logger = logging.getLogger("yaci_s3.validators.adapot_koios")

KOIOS_TOTALS_URL = "https://api.koios.rest/api/v1/totals"
KOIOS_FIELDS = ["treasury", "reserves", "fees", "deposits_stake"]
REQUEST_DELAY = 0.1  # 10 req/s Koios free tier


def _fetch_koios_totals(epoch: int) -> Optional[dict]:
    """Fetch Koios /totals for an epoch. Returns None on failure."""
    try:
        resp = requests.get(KOIOS_TOTALS_URL, params={"_epoch_no": epoch}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            logger.warning("Koios returned empty for epoch %d", epoch)
            return None
        return data[0]
    except Exception as e:
        logger.warning("Koios request failed for epoch %d: %s", epoch, e)
        return None


def validate_epoch(parquet_path: str, epoch: int) -> dict:
    """Validate one adapot epoch against Koios.

    Returns dict with:
        - "koios_fields": {field: {"parquet": val, "koios": val, "match": bool}}
        - "ratio_check": {"parquet_ratio": float, "expected": 0.8, "match": bool}
        - "all_pass": bool
        - "koios_available": bool
    """
    table = pq.ParquetFile(parquet_path).read()
    result = {
        "epoch": epoch,
        "koios_fields": {},
        "ratio_check": {},
        "all_pass": True,
        "koios_available": True,
    }

    # Fetch Koios data
    koios = _fetch_koios_totals(epoch)
    if koios is None:
        result["koios_available"] = False
        result["all_pass"] = False
        return result

    # Check 4 Koios fields
    for field in KOIOS_FIELDS:
        pq_val = table.column(field)[0].as_py()
        koios_val = int(koios.get(field, 0))
        match = pq_val == koios_val
        result["koios_fields"][field] = {
            "parquet": pq_val,
            "koios": koios_val,
            "match": match,
        }
        if not match:
            result["all_pass"] = False

    # Check pool_rewards_pot == rewards_pot * 0.8
    rewards_pot = table.column("rewards_pot")[0].as_py()
    pool_rewards_pot = table.column("pool_rewards_pot")[0].as_py()
    if rewards_pot and rewards_pot > 0:
        # Cardano ledger: treasury gets floor(rewards_pot/5), pool gets the rest
        expected_pool = rewards_pot - rewards_pot // 5
        ratio_match = pool_rewards_pot == expected_pool
        result["ratio_check"] = {
            "rewards_pot": rewards_pot,
            "pool_rewards_pot": pool_rewards_pot,
            "expected_pool": expected_pool,
            "match": ratio_match,
        }
        if not ratio_match:
            result["all_pass"] = False

    return result


def correct_epoch(parquet_path: str, koios_data: dict) -> str:
    """Rewrite parquet with Koios values for mismatched fields.

    Only corrects the 4 Koios-validatable fields. Returns the path written.
    """
    table = pq.ParquetFile(parquet_path).read()
    schema = table.schema

    for field in KOIOS_FIELDS:
        koios_val = int(koios_data.get(field, 0))
        field_type = schema.field(field).type
        new_col = pa.array([koios_val], type=field_type)
        idx = table.schema.get_field_index(field)
        table = table.set_column(idx, field, new_col)

    pq.write_table(table, parquet_path)
    return parquet_path


def validate_all(base_data_path: str, epochs: Optional[List[int]] = None) -> List[dict]:
    """Validate all adapot epochs. Returns list of mismatch results.

    Args:
        base_data_path: Path containing adapot/epoch=N/ directories.
        epochs: If None, discover all epochs from filesystem.
    """
    if epochs is None:
        adapot_dir = Path(base_data_path) / "adapot"
        if not adapot_dir.is_dir():
            logger.error("adapot directory not found: %s", adapot_dir)
            return []
        epoch_dirs = sorted(adapot_dir.iterdir())
        epochs = []
        for d in epoch_dirs:
            if d.name.startswith("epoch="):
                try:
                    epochs.append(int(d.name.split("=")[1]))
                except ValueError:
                    pass

    mismatches = []
    total = len(epochs)

    for i, epoch in enumerate(epochs):
        parquet_dir = Path(base_data_path) / "adapot" / f"epoch={epoch}"
        files = list(parquet_dir.glob("*.parquet"))
        if not files:
            logger.warning("No parquet file for epoch %d", epoch)
            continue

        result = validate_epoch(str(files[0]), epoch)

        if not result["all_pass"]:
            mismatches.append(result)
            logger.warning("Epoch %d: MISMATCH %s", epoch, _format_mismatches(result))
        else:
            if (i + 1) % 50 == 0 or i == total - 1:
                logger.info("Validated %d/%d epochs, %d mismatches so far",
                            i + 1, total, len(mismatches))

        time.sleep(REQUEST_DELAY)

    logger.info("Validation complete: %d epochs checked, %d mismatches",
                total, len(mismatches))
    return mismatches


def _format_mismatches(result: dict) -> str:
    """Format mismatch details for logging."""
    parts = []
    for field, info in result.get("koios_fields", {}).items():
        if not info["match"]:
            parts.append(f"{field}: parquet={info['parquet']} koios={info['koios']}")
    rc = result.get("ratio_check", {})
    if rc and not rc.get("match", True):
        parts.append(f"pool_ratio: got={rc['pool_rewards_pot']} expected={rc['expected_pool']}")
    return ", ".join(parts) if parts else "unknown"

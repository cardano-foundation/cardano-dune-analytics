"""Structured logging setup."""

import logging
import os
import sys
from datetime import datetime, timezone


def setup_logging(verbose: bool = False, log_dir: str = "logs") -> logging.Logger:
    """Configure logging to both stderr and a timestamped log file.

    Log files are written to {log_dir}/yaci-s3-{YYYY-MM-DD_HHMMSS}.log
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = logging.Formatter(fmt)

    root = logging.getLogger("yaci_s3")
    root.setLevel(level)

    # Console handler (stderr) — only INFO+ unless verbose
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # File handler — always DEBUG for full detail
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    log_path = os.path.join(log_dir, f"yaci-s3-{timestamp}.log")
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Suppress noisy boto/urllib loggers in file too
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root.info("Logging to file: %s", log_path)
    return root

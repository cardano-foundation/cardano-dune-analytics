"""Orchestrator: scan -> validate -> upload -> track."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

from .config import AppConfig
from .db import TrackingDB
from .models import ExporterDef, PartitionInfo, UploadRecord
from .scanner import scan_exporter
from .uploader import S3Uploader, UPLOAD_MAX_RETRIES
from .validator import validate_partition

logger = logging.getLogger("yaci_s3.orchestrator")


def _fmt_duration(seconds: float) -> str:
    """Format duration as human-readable string."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m{secs:.1f}s"


def process_exporter(
    exporter: ExporterDef,
    config: AppConfig,
    db: TrackingDB,
    uploader: S3Uploader,
    dry_run: bool = False,
    skip_validation: bool = False,
    partition_filter: Optional[set] = None,
) -> dict:
    """Process a single exporter: scan, validate, upload, track.

    Returns a summary dict with counts and timing.
    """
    exporter_start = time.monotonic()

    summary = {
        "exporter": exporter.name,
        "scanned": 0,
        "skipped_existing": 0,
        "validated": 0,
        "validation_errors": 0,
        "uploaded": 0,
        "upload_errors": 0,
        "timings": {},
    }

    # Step 1: Scan
    t0 = time.monotonic()
    partitions = scan_exporter(config.base_data_path, exporter)
    summary["scanned"] = len(partitions)
    scan_dur = time.monotonic() - t0
    summary["timings"]["scan"] = scan_dur
    logger.info("[%s] Scan: found %d partitions (%s)", exporter.name, len(partitions), _fmt_duration(scan_dur))

    # Step 1b: Apply partition filter if specified
    if partition_filter:
        partitions = [p for p in partitions if p.partition_value in partition_filter]
        logger.info("[%s] Partition filter: %d matched from %s", exporter.name, len(partitions), partition_filter)

    # Step 2: Filter already uploaded
    t0 = time.monotonic()
    uploaded_set = db.get_uploaded_partitions(exporter.name)
    new_partitions = [p for p in partitions if p.partition_value not in uploaded_set]
    summary["skipped_existing"] = len(partitions) - len(new_partitions)
    filter_dur = time.monotonic() - t0
    summary["timings"]["filter"] = filter_dur
    logger.info(
        "[%s] Filter: %d new, %d already uploaded (%s)",
        exporter.name, len(new_partitions), summary["skipped_existing"], _fmt_duration(filter_dur),
    )

    if not new_partitions:
        total_dur = time.monotonic() - exporter_start
        summary["timings"]["total"] = total_dur
        logger.info("[%s] No new partitions, done (%s total)", exporter.name, _fmt_duration(total_dur))
        return summary

    total_validate_dur = 0.0
    total_upload_dur = 0.0

    for i, partition in enumerate(new_partitions, 1):
        part_label = f"{exporter.name}/{partition.partition_value}"

        # Step 3: Validate
        if not skip_validation:
            t0 = time.monotonic()
            result = validate_partition(partition, exporter, config)
            v_dur = time.monotonic() - t0
            total_validate_dur += v_dur
            summary["validated"] += 1

            if not result.is_valid:
                logger.warning(
                    "[%s] FAIL validate %s: %s (%s)",
                    exporter.name, partition.partition_value, result.error_details, _fmt_duration(v_dur),
                )
                if not dry_run:
                    db.record_validation_error(result)
                summary["validation_errors"] += 1
                continue

            logger.info(
                "[%s] OK validate %s: rows=%d, slots=[%s,%s] (%s)",
                exporter.name, partition.partition_value,
                result.pq_stats.row_count,
                result.pq_stats.min_slot, result.pq_stats.max_slot,
                _fmt_duration(v_dur),
            )
            pq_stats = result.pq_stats
        else:
            pq_stats = None

        # Step 4: Upload
        t0 = time.monotonic()
        s3_key = uploader.upload(partition, dry_run=dry_run)
        u_dur = time.monotonic() - t0
        total_upload_dur += u_dur

        if s3_key is None:
            logger.error("[%s] FAIL upload %s (%s)", exporter.name, partition.partition_value, _fmt_duration(u_dur))
            db.record_upload_error(
                exporter=exporter.name,
                partition_value=partition.partition_value,
                file_path=partition.file_path,
                file_size=partition.file_size,
                error_details=f"Upload failed after {UPLOAD_MAX_RETRIES} attempts ({_fmt_duration(u_dur)})",
                attempts=UPLOAD_MAX_RETRIES,
            )
            summary["upload_errors"] += 1
            continue

        size_mb = partition.file_size / (1024 * 1024)
        logger.info(
            "[%s] OK upload %s [%d/%d]: %.1fMB -> %s (%s)",
            exporter.name, partition.partition_value,
            i, len(new_partitions), size_mb, s3_key, _fmt_duration(u_dur),
        )

        # Step 5: Record in tracking DB (skip in dry-run)
        if not dry_run:
            row_count = pq_stats.row_count if pq_stats else 0
            min_slot = pq_stats.min_slot if pq_stats else None
            max_slot = pq_stats.max_slot if pq_stats else None

            db.record_upload(UploadRecord(
                exporter=exporter.name,
                partition_value=partition.partition_value,
                s3_key=s3_key,
                file_name=Path(partition.file_path).name,
                row_count=row_count,
                min_slot=min_slot,
                max_slot=max_slot,
                file_size=partition.file_size,
            ))
        summary["uploaded"] += 1

    total_dur = time.monotonic() - exporter_start
    summary["timings"]["validate"] = total_validate_dur
    summary["timings"]["upload"] = total_upload_dur
    summary["timings"]["total"] = total_dur

    logger.info(
        "[%s] Complete: %d uploaded, %d errors, %d validation failures | "
        "scan=%s, validate=%s, upload=%s, total=%s",
        exporter.name,
        summary["uploaded"], summary["upload_errors"], summary["validation_errors"],
        _fmt_duration(scan_dur),
        _fmt_duration(total_validate_dur),
        _fmt_duration(total_upload_dur),
        _fmt_duration(total_dur),
    )

    return summary


def run(
    config: AppConfig,
    exporter_names: Optional[List[str]] = None,
    parallel: int = 1,
    dry_run: bool = False,
    skip_validation: bool = False,
    partition_filter: Optional[set] = None,
):
    """Run the upload pipeline for specified exporters."""
    pipeline_start = time.monotonic()

    db = TrackingDB(config.sqlite_path)
    uploader = S3Uploader(config.s3_bucket, aws_profile=config.aws_profile)

    if exporter_names:
        exporters = []
        for name in exporter_names:
            if name not in config.exporters:
                logger.error("Unknown exporter: '%s'", name)
                continue
            exporters.append(config.exporters[name])
    else:
        exporters = list(config.exporters.values())

    if not exporters:
        logger.error("No exporters to process")
        return

    if partition_filter:
        logger.info(
            "Starting pipeline: %d exporters, partitions=%s, dry_run=%s, skip_validation=%s",
            len(exporters), partition_filter, dry_run, skip_validation,
        )
    else:
        logger.info(
            "Starting pipeline: %d exporters, parallel=%d, dry_run=%s, skip_validation=%s",
            len(exporters), parallel, dry_run, skip_validation,
        )

    summaries = []

    if parallel <= 1:
        for exp in exporters:
            s = process_exporter(exp, config, db, uploader, dry_run, skip_validation, partition_filter)
            summaries.append(s)
    else:
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futures = {
                pool.submit(
                    process_exporter, exp, config,
                    db, uploader, dry_run, skip_validation, partition_filter,
                ): exp.name
                for exp in exporters
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    s = future.result()
                    summaries.append(s)
                except Exception as e:
                    logger.error("Exporter '%s' failed: %s", name, e)
                    summaries.append({"exporter": name, "error": str(e)})

    db.close()

    pipeline_dur = time.monotonic() - pipeline_start

    # Print summary
    logger.info("=" * 70)
    logger.info("Pipeline complete in %s. Summary:", _fmt_duration(pipeline_dur))
    logger.info("-" * 70)
    for s in sorted(summaries, key=lambda x: x["exporter"]):
        if "error" in s:
            logger.info("  %-30s FAILED: %s", s["exporter"], s["error"])
        else:
            timings = s.get("timings", {})
            logger.info(
                "  %-30s scanned=%-3d new=%-3d validated=%-3d errors=%-3d uploaded=%-3d  [%s]",
                s["exporter"],
                s["scanned"],
                s["scanned"] - s["skipped_existing"],
                s["validated"],
                s["validation_errors"],
                s["uploaded"],
                _fmt_duration(timings.get("total", 0)),
            )
    logger.info("=" * 70)

    return summaries


def retry_failed(
    config: AppConfig,
    exporter_filter: Optional[str] = None,
    dry_run: bool = False,
    skip_validation: bool = False,
):
    """Retry all failed uploads and validation errors.

    Reads from upload_errors and validation_errors tables, groups by exporter,
    and re-runs the pipeline for those specific partitions.
    """
    pipeline_start = time.monotonic()

    db = TrackingDB(config.sqlite_path)
    failures = db.get_failed_partitions(exporter_filter)

    if not failures:
        logger.info("No failed partitions to retry")
        db.close()
        return

    total_partitions = sum(len(parts) for parts in failures.values())
    logger.info(
        "Retrying %d failed partitions across %d exporters",
        total_partitions, len(failures),
    )
    for exp_name, parts in sorted(failures.items()):
        logger.info("  %s: %s", exp_name, sorted(parts))

    uploader = S3Uploader(config.s3_bucket, aws_profile=config.aws_profile)
    summaries = []

    for exporter_name, partition_values in sorted(failures.items()):
        if exporter_name not in config.exporters:
            logger.warning("Skipping unknown exporter in error records: '%s'", exporter_name)
            continue

        exporter = config.exporters[exporter_name]
        s = process_exporter(
            exporter, config, db, uploader,
            dry_run=dry_run,
            skip_validation=skip_validation,
            partition_filter=partition_values,
        )
        summaries.append(s)

        # Clear error records for successfully uploaded partitions
        if not dry_run:
            for pv in partition_values:
                uploaded = db.get_uploaded_partitions(exporter_name)
                if pv in uploaded:
                    db.clear_errors_for_partition(exporter_name, pv)
                    logger.info("[%s] Cleared error records for %s after successful retry", exporter_name, pv)

    db.close()

    pipeline_dur = time.monotonic() - pipeline_start
    logger.info("=" * 70)
    logger.info("Retry complete in %s. Summary:", _fmt_duration(pipeline_dur))
    logger.info("-" * 70)
    for s in sorted(summaries, key=lambda x: x["exporter"]):
        if "error" in s:
            logger.info("  %-30s FAILED: %s", s["exporter"], s["error"])
        else:
            timings = s.get("timings", {})
            logger.info(
                "  %-30s uploaded=%-3d errors=%-3d validation_errors=%-3d  [%s]",
                s["exporter"],
                s["uploaded"], s["upload_errors"], s["validation_errors"],
                _fmt_duration(timings.get("total", 0)),
            )
    logger.info("=" * 70)

    return summaries


def run_external(
    config: AppConfig,
    exporter_names: List[str],
    dry_run: bool = False,
):
    """Run external API-based exporters."""
    from .external import EXTERNAL_EXPORTERS

    pipeline_start = time.monotonic()
    db = TrackingDB(config.sqlite_path)
    uploader = S3Uploader(config.s3_bucket, aws_profile=config.aws_profile)

    summaries = []
    for name in exporter_names:
        if name not in EXTERNAL_EXPORTERS:
            logger.error("Unknown external exporter: '%s'. Available: %s",
                         name, list(EXTERNAL_EXPORTERS.keys()))
            continue

        exporter_cls = EXTERNAL_EXPORTERS[name]
        exporter = exporter_cls(config, db, uploader)
        summary = exporter.run(dry_run=dry_run)
        summaries.append(summary)

    db.close()

    pipeline_dur = time.monotonic() - pipeline_start
    logger.info("=" * 70)
    logger.info("External pipeline complete in %s. Summary:", _fmt_duration(pipeline_dur))
    logger.info("-" * 70)
    for s in summaries:
        status = s.get("status", "unknown")
        if status == "failed":
            logger.info("  %-30s FAILED: %s", s["exporter"], s.get("error", ""))
        else:
            logger.info(
                "  %-30s fetched=%-5d exported=%-5d",
                s["exporter"], s["records_fetched"], s["records_exported"],
            )
    logger.info("=" * 70)

    return summaries


def run_internal(
    config: AppConfig,
    job_name: str,
    rebuild: bool = False,
    dates: Optional[List[str]] = None,
    dry_run: bool = False,
):
    """Run an internal job (e.g., drep_profile)."""
    from .internal import INTERNAL_JOBS

    if job_name not in INTERNAL_JOBS:
        logger.error("Unknown internal job: '%s'. Available: %s",
                      job_name, list(INTERNAL_JOBS.keys()))
        return None

    pipeline_start = time.monotonic()
    job_cls = INTERNAL_JOBS[job_name]
    job = job_cls(config)
    summary = job.run(rebuild=rebuild, dates=dates, dry_run=dry_run)

    pipeline_dur = time.monotonic() - pipeline_start
    logger.info("=" * 70)
    logger.info("Internal job '%s' complete in %s", job_name, _fmt_duration(pipeline_dur))
    if summary:
        for k, v in summary.items():
            if k not in ("job",):
                logger.info("  %-25s %s", k, v)
    logger.info("=" * 70)

    return summary


def run_hybrid(
    config: AppConfig,
    exporter_names: List[str],
    dry_run: bool = False,
    partition_filter: Optional[set] = None,
):
    """Run hybrid exporters."""
    from .hybrid import HYBRID_EXPORTERS

    pipeline_start = time.monotonic()
    db = TrackingDB(config.sqlite_path)
    uploader = S3Uploader(config.s3_bucket, aws_profile=config.aws_profile)

    summaries = []
    for name in exporter_names:
        if name not in HYBRID_EXPORTERS:
            logger.error("Unknown hybrid exporter: '%s'. Available: %s",
                          name, list(HYBRID_EXPORTERS.keys()))
            continue

        exporter_cls = HYBRID_EXPORTERS[name]
        exporter = exporter_cls(config, db, uploader)
        summary = exporter.run(dry_run=dry_run, partition_filter=partition_filter)
        summaries.append(summary)

    db.close()

    pipeline_dur = time.monotonic() - pipeline_start
    logger.info("=" * 70)
    logger.info("Hybrid pipeline complete in %s. Summary:", _fmt_duration(pipeline_dur))
    logger.info("-" * 70)
    for s in summaries:
        status = s.get("status", "unknown")
        if status == "failed":
            logger.info("  %-30s FAILED: %s", s["exporter"], s.get("error", ""))
        else:
            logger.info(
                "  %-30s scanned=%-3d enriched=%-3d uploaded=%-3d errors=%-3d",
                s["exporter"], s["scanned"], s["enriched"], s["uploaded"], s["errors"],
            )
    logger.info("=" * 70)

    return summaries


def rebuild_tracking(config: AppConfig):
    """Rebuild SQLite tracking DB from S3 bucket contents."""
    t0 = time.monotonic()
    db = TrackingDB(config.sqlite_path)
    uploader = S3Uploader(config.s3_bucket, aws_profile=config.aws_profile)
    objects = uploader.list_all_objects()
    db.rebuild_from_s3(objects)
    db.close()
    dur = time.monotonic() - t0
    logger.info("Tracking database rebuilt with %d records (%s)", len(objects), _fmt_duration(dur))

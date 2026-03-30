"""Click CLI for yaci-s3."""

import sys
from datetime import date, timedelta

import click

from .config import load_config
from .logging_setup import setup_logging


def _expand_range(range_str: str) -> list:
    """Expand a range string like '2024-01-01:2024-01-05' or '400:410' into individual values.

    Supports:
      - Date ranges: 2024-01-01:2024-01-05 (inclusive, generates each day)
      - Epoch ranges: 400:410 (inclusive, generates each integer)
    """
    if ":" not in range_str:
        raise click.BadParameter(f"Range must contain ':' separator, got '{range_str}'")

    start, end = range_str.split(":", 1)

    # Try epoch (integer) range first
    try:
        start_int = int(start)
        end_int = int(end)
        if start_int > end_int:
            raise click.BadParameter(f"Range start ({start}) must be <= end ({end})")
        return [str(i) for i in range(start_int, end_int + 1)]
    except ValueError:
        pass

    # Try date range
    try:
        start_date = date.fromisoformat(start)
        end_date = date.fromisoformat(end)
    except ValueError:
        raise click.BadParameter(
            f"Invalid range '{range_str}'. Use date format YYYY-MM-DD:YYYY-MM-DD or epoch format N:N"
        )

    if start_date > end_date:
        raise click.BadParameter(f"Range start ({start}) must be <= end ({end})")

    values = []
    current = start_date
    while current <= end_date:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


@click.command("yaci-s3")
@click.option("--all", "run_all", is_flag=True, help="Process all exporters")
@click.option("--dune", "run_dune", is_flag=True, help="Process all 'dune' group exporters")
@click.option("--exporter", "exporter_name", type=str, help="Process a single exporter by name")
@click.option("--partition", "partitions", type=str, multiple=True,
              help="Only process specific partitions (dates or epochs). Repeatable: --partition 2024-01-15 --partition 2024-01-16")
@click.option("--range", "partition_range", type=str, default=None,
              help="Process a range of partitions (inclusive). Dates: --range 2024-01-01:2024-01-31  Epochs: --range 400:410")
@click.option("--retry-failures", is_flag=True, help="Retry all failed uploads and validation errors")
@click.option("--parallel", type=int, default=1, help="Number of concurrent exporters")
@click.option("--dry-run", is_flag=True, help="Validate only, skip uploads")
@click.option("--skip-validation", is_flag=True, help="Skip PG validation")
@click.option("--rebuild-tracking", is_flag=True, help="Rebuild SQLite tracking from S3")
@click.option("--external", "external_name", type=click.Choice(["asset_data", "smart_contract_registry", "off_chain_pool_data"]),
              help="Run a single external exporter")
@click.option("--external-all", is_flag=True, help="Run all external exporters")
@click.option("--hybrid", "hybrid_name", type=str, default=None,
              help="Run a single hybrid exporter (e.g. drep_dist_enriched)")
@click.option("--hybrid-all", is_flag=True, help="Run all hybrid exporters")
@click.option("--internal", "internal_name", type=str, default=None,
              help="Run an internal job (e.g. drep_profile)")
@click.option("--rebuild", is_flag=True, help="Rebuild from scratch (for internal jobs and off_chain_pool_data)")
@click.option("--date", "single_date", type=str, default=None,
              help="Process a specific date (for internal jobs, YYYY-MM-DD)")
@click.option("--start-date", type=str, default=None,
              help="Start date for internal job date range (YYYY-MM-DD)")
@click.option("--end-date", type=str, default=None,
              help="End date for internal job date range (YYYY-MM-DD)")
@click.option("--validate-adapot", is_flag=True, help="Validate adapot data against Koios API")
@click.option("--env-file", type=str, default=".env", help="Path to .env file")
@click.option("--exporters-file", type=str, default="exporters.json", help="Path to exporters.json")
@click.option("--verbose", is_flag=True, help="Enable debug logging")
def main(
    run_all,
    run_dune,
    exporter_name,
    partitions,
    partition_range,
    retry_failures,
    parallel,
    dry_run,
    skip_validation,
    rebuild_tracking,
    external_name,
    external_all,
    hybrid_name,
    hybrid_all,
    internal_name,
    rebuild,
    single_date,
    start_date,
    end_date,
    validate_adapot,
    env_file,
    exporters_file,
    verbose,
):
    """S3 upload tool for yaci-store parquet exports."""
    logger = setup_logging(verbose)

    # External/hybrid/internal/validate exporters don't need PG
    require_pg = not (external_name or external_all or hybrid_name or hybrid_all or internal_name or validate_adapot)

    try:
        config = load_config(env_file, exporters_file, require_pg=require_pg)
    except (ValueError, FileNotFoundError) as e:
        logger.error("Configuration error: %s", e)
        sys.exit(1)

    # --- Validate adapot path ---
    if validate_adapot:
        from .validators.adapot_koios import validate_all
        mismatches = validate_all(config.base_data_path)
        if mismatches:
            logger.error("Adapot validation: %d epochs with mismatches", len(mismatches))
            sys.exit(1)
        else:
            logger.info("Adapot validation: all epochs passed")
        return

    # --- Internal job path ---
    if internal_name:
        from .orchestrator import run_internal
        dates = None
        if single_date:
            dates = [single_date]
        elif start_date and end_date:
            try:
                sd = date.fromisoformat(start_date)
                ed = date.fromisoformat(end_date)
            except ValueError:
                logger.error("Invalid date format. Use YYYY-MM-DD")
                sys.exit(1)
            if sd > ed:
                logger.error("start-date must be <= end-date")
                sys.exit(1)
            dates = []
            current = sd
            while current <= ed:
                dates.append(current.isoformat())
                current += timedelta(days=1)
        elif not rebuild:
            logger.error("Internal job requires --rebuild, --date, or --start-date/--end-date")
            sys.exit(1)
        run_internal(config=config, job_name=internal_name, rebuild=rebuild, dates=dates, dry_run=dry_run)
        return

    # --- Hybrid exporter path ---
    if hybrid_name or hybrid_all:
        from .orchestrator import run_hybrid
        if hybrid_all:
            from .hybrid import HYBRID_EXPORTERS
            names = list(HYBRID_EXPORTERS.keys())
        else:
            names = [hybrid_name]

        partition_filter_h = set(partitions) if partitions else set()
        if partition_range:
            try:
                expanded = _expand_range(partition_range)
                partition_filter_h.update(expanded)
            except click.BadParameter as e:
                logger.error("Invalid --range: %s", e.format_message())
                sys.exit(1)
        partition_filter_h = partition_filter_h if partition_filter_h else None

        run_hybrid(config=config, exporter_names=names, dry_run=dry_run, partition_filter=partition_filter_h)
        return

    # --- External exporter path ---
    if external_name or external_all:
        from .orchestrator import run_external
        if external_all:
            from .external import EXTERNAL_EXPORTERS
            names = list(EXTERNAL_EXPORTERS.keys())
        else:
            names = [external_name]
        run_external(config=config, exporter_names=names, dry_run=dry_run, rebuild=rebuild)
        return

    if rebuild_tracking:
        from .orchestrator import rebuild_tracking as do_rebuild
        do_rebuild(config)
        return

    if retry_failures:
        from .orchestrator import retry_failed
        exporter_filter = exporter_name if exporter_name else None
        retry_failed(
            config=config,
            exporter_filter=exporter_filter,
            dry_run=dry_run,
            skip_validation=skip_validation,
        )
        return

    # Determine which exporters to run
    if exporter_name:
        exporter_names = [exporter_name]
    elif run_dune:
        exporter_names = [
            name for name, exp in config.exporters.items() if exp.group == "dune"
        ]
    elif run_all:
        exporter_names = list(config.exporters.keys())
    else:
        logger.error("Specify --all, --dune, --exporter, --external, --external-all, --hybrid, --hybrid-all, or --internal")
        sys.exit(1)

    # Build partition filter from --partition and --range
    partition_filter = set(partitions) if partitions else set()

    if partition_range:
        try:
            expanded = _expand_range(partition_range)
            partition_filter.update(expanded)
            logger.info("Range %s expanded to %d partitions", partition_range, len(expanded))
        except click.BadParameter as e:
            logger.error("Invalid --range: %s", e.format_message())
            sys.exit(1)

    partition_filter = partition_filter if partition_filter else None

    from .orchestrator import run
    run(
        config=config,
        exporter_names=exporter_names,
        parallel=parallel,
        dry_run=dry_run,
        skip_validation=skip_validation,
        partition_filter=partition_filter,
    )


if __name__ == "__main__":
    main()

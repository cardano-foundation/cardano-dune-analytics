# yaci-s3

S3 upload tool for yaci-store Cardano parquet exports. Validates parquet files against PostgreSQL and uploads to S3 with SQLite-based tracking.

## Setup

```bash
# Install uv if needed: https://docs.astral.sh/uv/getting-started/installation/
uv sync
cp .env.example .env
# Edit .env with your actual values
```

## Configuration

### `.env`

| Variable | Description | Default |
|----------|-------------|---------|
| `PG_HOST` | PostgreSQL host | (required) |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_DB` | PostgreSQL database | (required) |
| `PG_USER` | PostgreSQL user | (required) |
| `PG_PASSWORD` | PostgreSQL password | (required) |
| `PG_SCHEMA` | PostgreSQL schema | `public` |
| `S3_BUCKET` | S3 bucket name | (required) |
| `AWS_PROFILE` | AWS CLI profile name for credentials | (optional) |
| `BASE_DATA_PATH` | Local path to parquet exports | (required) |
| `SQLITE_PATH` | Path to tracking database | `./uploads.db` |

### `exporters.json`

Defines the 12 dune-group exporters (9 daily, 3 epoch). Each entry specifies the exporter name, PG table, slot/epoch column, and partition type.

### AWS Credentials

Credentials can be provided via:
- **AWS profile** (recommended): Set `AWS_PROFILE` in `.env` and configure `~/.aws/credentials`
- **Environment variables**: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- **IAM role**: When running on EC2/ECS with an attached role

## Usage

### Basic Operations

```bash
# Validate + upload all dune exporters, 4 concurrent
uv run yaci-s3 --dune --parallel 4

# Single exporter
uv run yaci-s3 --exporter block

# All exporters
uv run yaci-s3 --all

# Dry run (validate only, no uploads)
uv run yaci-s3 --dune --dry-run

# Skip PG validation (upload only)
uv run yaci-s3 --dune --skip-validation

# Debug logging
uv run yaci-s3 --all --verbose
```

### Uploading Specific Partitions

Use `--partition` to target individual dates/epochs, or `--range` to target a continuous range. Both can be combined.

```bash
# Upload a single date partition
uv run yaci-s3 --exporter block --partition 2024-01-15

# Upload multiple specific dates
uv run yaci-s3 --exporter block --partition 2024-01-15 --partition 2024-01-16

# Upload a specific epoch partition
uv run yaci-s3 --exporter reward --partition 500

# Combine with skip-validation for a quick re-upload
uv run yaci-s3 --exporter block --partition 2021-06-05 --skip-validation

# Works with --dune or --all to filter across multiple exporters
uv run yaci-s3 --dune --partition 2024-01-15
```

### Uploading a Range of Partitions

Use `--range START:END` (inclusive) to process a continuous range of dates or epochs without listing each one individually. The separator is `:`.

**Date ranges** (for daily exporters):

```bash
# Upload all of January 2024
uv run yaci-s3 --exporter block --range 2024-01-01:2024-01-31

# Upload a week's worth of data
uv run yaci-s3 --exporter transaction --range 2023-06-01:2023-06-07

# Upload June 2023 onwards (e.g. resuming after a completed backfill through May)
uv run yaci-s3 --exporter block --range 2023-06-01:2023-12-31
```

**Epoch ranges** (for epoch exporters):

```bash
# Upload epochs 400 through 410
uv run yaci-s3 --exporter reward --range 400:410

# Upload a single epoch (equivalent to --partition 500)
uv run yaci-s3 --exporter epoch_stake --range 500:500
```

**Combining `--partition` and `--range`:**

```bash
# Range plus individual dates — both are merged into one filter
uv run yaci-s3 --exporter block --range 2024-01-01:2024-01-31 --partition 2023-12-25

# This processes Jan 1-31 2024 AND Dec 25 2023
```

Only partitions that exist locally as Hive-style directories AND match the filter will be processed. Partitions already recorded as uploaded in the tracking DB are still skipped unless you clear the DB entry first.

### Retrying Failures

Failed uploads and validation errors are recorded in the SQLite tracking database. Use `--retry-failures` to automatically re-process all previously failed partitions.

```bash
# Retry all failures across all exporters
uv run yaci-s3 --retry-failures

# Retry failures for a specific exporter only
uv run yaci-s3 --retry-failures --exporter block

# Retry with skip-validation (useful when PG data has since been corrected)
uv run yaci-s3 --retry-failures --skip-validation

# Dry run retry to see what would be retried
uv run yaci-s3 --retry-failures --dry-run
```

When a retry succeeds, the corresponding error records are automatically cleared from the `upload_errors` and `validation_errors` tables. Partitions that have already been successfully uploaded (e.g., via `--partition`) are excluded from the retry set.

### Rebuilding Tracking Database

If the SQLite tracking database is lost or corrupted, rebuild it from the S3 bucket contents:

```bash
uv run yaci-s3 --rebuild-tracking
```

This lists all objects in the S3 bucket, parses the keys to extract exporter and partition metadata, and repopulates the `uploads` table. Row count and slot range metadata is not available from S3 and will be set to defaults (0/null).

### External Data Exporters

External exporters fetch data from HTTP APIs (not PostgreSQL), produce parquet files, and upload to S3. They share the same S3 uploader and SQLite tracking DB but have their own fetch-write-upload cycle.

**Available external exporters:**

| Exporter | Source | Schedule | Description |
|----------|--------|----------|-------------|
| `asset_data` | Minswap API | Every 2 hours | Full snapshot of all verified Cardano tokens with price data |
| `contract_registry` | GitHub repos | Daily | Incremental export of new smart contract script hashes |

```bash
# Run a single external exporter
uv run yaci-s3 --external asset_data

# Run all external exporters
uv run yaci-s3 --external-all

# Dry run (fetch + write parquet locally, skip S3 upload)
uv run yaci-s3 --external asset_data --dry-run

# Verbose logging
uv run yaci-s3 --external contract_registry --verbose
```

External exporters **do not** require PostgreSQL credentials — only `S3_BUCKET` and `BASE_DATA_PATH` are needed.

#### Additional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GITHUB_TOKEN` | GitHub personal access token (raises rate limit from 60 to 5000 req/hour) | (optional) |
| `MINSWAP_REQUEST_DELAY` | Delay in seconds between Minswap API pages | `5.0` |
| `MINSWAP_MAX_RETRIES` | Max retries for failed Minswap API requests | `5` |

#### File Naming

External exporters use a `.N` suffix to allow multiple snapshots per day:

- **S3 key**: `{exporter}/{YYYY-MM-DD}/{exporter}-{YYYY-MM-DD}.{N}.parquet`
- **Local**: `{BASE_DATA_PATH}/{exporter}/date={YYYY-MM-DD}/{exporter}-{YYYY-MM-DD}.{N}.parquet`
- **Tracking DB partition_value**: `{YYYY-MM-DD}.{N}` (e.g., `2024-01-15.1`, `2024-01-15.2`)

The suffix N is the count of existing uploads for that date + 1, queried from the tracking DB.

#### Scheduling

External exporters are intended to be invoked by an external scheduler (cron/systemd timer):

```bash
# asset_data every 2 hours
0 */2 * * * cd /path/to/s3-uploader && uv run yaci-s3 --external asset_data

# contract_registry daily at 6am UTC
0 6 * * * cd /path/to/s3-uploader && uv run yaci-s3 --external contract_registry
```

## CLI Reference

| Option | Description |
|--------|-------------|
| `--all` | Process all exporters defined in `exporters.json` |
| `--dune` | Process exporters in the "dune" group |
| `--exporter NAME` | Process a single exporter by name |
| `--partition VALUE` | Only process specific partitions (dates or epochs). Repeatable. |
| `--range START:END` | Process a range of partitions (inclusive). Dates: `2024-01-01:2024-01-31`. Epochs: `400:410`. |
| `--retry-failures` | Retry all previously failed uploads and validation errors |
| `--parallel N` | Number of concurrent exporters (default: 1) |
| `--dry-run` | Validate only, skip actual S3 uploads |
| `--skip-validation` | Skip PostgreSQL validation checks |
| `--rebuild-tracking` | Rebuild SQLite tracking DB from S3 bucket listing |
| `--external NAME` | Run a single external exporter (`asset_data` or `contract_registry`) |
| `--external-all` | Run all external exporters |
| `--env-file PATH` | Path to `.env` file (default: `.env`) |
| `--exporters-file PATH` | Path to exporters config (default: `exporters.json`) |
| `--verbose` | Enable debug-level logging |

### Option Combinations

| Scenario | Command |
|----------|---------|
| Full backfill, all dune exporters | `uv run yaci-s3 --dune --parallel 4` |
| Single exporter, specific dates | `uv run yaci-s3 --exporter block --partition 2024-01-15 --partition 2024-01-16` |
| Upload a full month | `uv run yaci-s3 --exporter block --range 2024-01-01:2024-01-31` |
| Upload epoch range | `uv run yaci-s3 --exporter reward --range 400:410` |
| Validate without uploading | `uv run yaci-s3 --dune --dry-run` |
| Upload without validation | `uv run yaci-s3 --exporter block --skip-validation` |
| Retry all failures | `uv run yaci-s3 --retry-failures` |
| Retry one exporter's failures, skip validation | `uv run yaci-s3 --retry-failures --exporter block --skip-validation` |
| Re-upload a single failed partition | `uv run yaci-s3 --exporter block --partition 2021-06-05 --skip-validation` |
| Recover tracking after DB loss | `uv run yaci-s3 --rebuild-tracking` |
| Run asset_data external exporter | `uv run yaci-s3 --external asset_data` |
| Run all external exporters | `uv run yaci-s3 --external-all` |
| Dry run external exporter | `uv run yaci-s3 --external asset_data --dry-run` |

## How It Works

### Pipeline Flow (per exporter)

1. **Scan** local Hive-style partition directories (`date=YYYY-MM-DD` or `epoch=N`)
2. **Filter** by `--partition` values if specified
3. **Skip** partitions already recorded in SQLite tracking DB
4. For each new partition:
   - **Validate** parquet against PostgreSQL (unless `--skip-validation`)
   - **Upload** to S3 with retry (10 attempts, exponential backoff: 10s, 20s, 40s, 80s, 120s cap)
   - **Verify** upload via HEAD request + file size check
   - **Record** success in SQLite `uploads` table
5. On failure: record to `upload_errors` or `validation_errors` table, skip partition, continue

### Validation

**Daily exporters**: DuckDB reads parquet to get row count and slot range. These are compared against:
- PG `block` table: slot range for the given date (using `block_time`)
- PG exporter table: row count within the parquet's slot range

**Epoch exporters**: DuckDB row count is compared against PG exporter table count for the given epoch.

Mismatches are logged to the `validation_errors` table and the partition is skipped (not uploaded).

### S3 Path Convention

- Daily: `{exporter}/{YYYY-MM-DD}/{exporter}-{YYYY-MM-DD}.parquet`
- Epoch: `{exporter}/{N}/{exporter}-epoch-{N}.parquet`
- External: `{exporter}/{YYYY-MM-DD}/{exporter}-{YYYY-MM-DD}.{N}.parquet`

### SQLite Tracking Database

The tool maintains a SQLite database (default: `./uploads.db`) with three tables to track uploads and errors. The database is created automatically on first run.

#### Table: `uploads`

Tracks successfully uploaded partitions. The `(exporter, partition_value)` pair is unique — re-uploading the same partition overwrites the previous record.

```sql
CREATE TABLE uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter        TEXT NOT NULL,           -- e.g. "block", "transaction"
    partition_value TEXT NOT NULL,           -- e.g. "2024-01-15" or "500"
    s3_key          TEXT NOT NULL,           -- full S3 object key
    file_name       TEXT NOT NULL,           -- local filename
    row_count       INTEGER NOT NULL,        -- parquet row count (0 if validation skipped)
    min_slot        INTEGER,                 -- min slot from parquet (NULL for epoch exporters)
    max_slot        INTEGER,                 -- max slot from parquet (NULL for epoch exporters)
    file_size       INTEGER NOT NULL,        -- file size in bytes
    uploaded_at     TEXT NOT NULL,           -- ISO 8601 UTC timestamp
    status          TEXT NOT NULL DEFAULT 'completed',
    UNIQUE(exporter, partition_value)
);
```

#### Table: `upload_errors`

Records failed S3 uploads. Multiple error records can exist for the same partition (one per failed run). Cleared automatically on successful retry via `--retry-failures`.

```sql
CREATE TABLE upload_errors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter        TEXT NOT NULL,           -- exporter name
    partition_value TEXT NOT NULL,           -- partition that failed
    file_path       TEXT,                    -- local file path attempted
    file_size       INTEGER,                 -- file size in bytes
    error_details   TEXT,                    -- error message with duration
    attempts        INTEGER,                 -- number of upload attempts made
    created_at      TEXT NOT NULL            -- ISO 8601 UTC timestamp
);
```

#### Table: `validation_errors`

Records validation mismatches between parquet files and PostgreSQL. Cleared automatically on successful retry via `--retry-failures`.

```sql
CREATE TABLE validation_errors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter        TEXT NOT NULL,           -- exporter name
    partition_value TEXT NOT NULL,           -- partition that failed validation
    pq_count        INTEGER,                -- parquet row count
    pg_count        INTEGER,                -- PostgreSQL row count
    pq_min_slot     INTEGER,                -- parquet min slot
    pq_max_slot     INTEGER,                -- parquet max slot
    pg_min_slot     INTEGER,                -- PostgreSQL min slot
    pg_max_slot     INTEGER,                -- PostgreSQL max slot
    error_details   TEXT,                    -- mismatch description
    created_at      TEXT NOT NULL            -- ISO 8601 UTC timestamp
);
```

#### Table: `external_exporter_runs`

Tracks run history for external exporters.

```sql
CREATE TABLE external_exporter_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter         TEXT NOT NULL,           -- e.g. "asset_data", "contract_registry"
    run_started_at   TEXT NOT NULL,           -- ISO 8601 UTC timestamp
    run_completed_at TEXT,                    -- ISO 8601 UTC timestamp
    records_fetched  INTEGER,                -- rows fetched from API
    records_exported INTEGER,                -- rows written to parquet
    status           TEXT NOT NULL DEFAULT 'running',  -- running, completed, failed
    error_details    TEXT                     -- error message if failed
);
```

#### Table: `contract_registry_state`

Tracks the last processed commit SHA per GitHub source for incremental contract registry updates.

```sql
CREATE TABLE contract_registry_state (
    source          TEXT PRIMARY KEY,        -- e.g. "stricahq", "crfa_v2", "crfa_v1"
    last_commit_sha TEXT NOT NULL,           -- latest processed commit SHA
    last_checked_at TEXT NOT NULL            -- ISO 8601 UTC timestamp
);
```

#### Table: `contract_registry_hashes`

Stores all known script hashes to enable incremental exports (only new hashes are exported).

```sql
CREATE TABLE contract_registry_hashes (
    script_hash  TEXT PRIMARY KEY,          -- the contract script hash
    source       TEXT NOT NULL,             -- which source provided it first
    first_seen_at TEXT NOT NULL             -- ISO 8601 UTC timestamp
);
```

#### Querying the Database

```bash
# Open the database
sqlite3 uploads.db

# Enable column headers and table mode for readable output
.headers on
.mode column
```

```sql
-- View uploaded partitions for an exporter
SELECT partition_value, uploaded_at, file_size
FROM uploads WHERE exporter = 'block'
ORDER BY partition_value;

-- Count uploads per exporter
SELECT exporter, COUNT(*) as total, SUM(file_size) / (1024*1024*1024.0) as total_gb
FROM uploads GROUP BY exporter ORDER BY exporter;

-- View upload failures
SELECT exporter, partition_value, error_details, attempts, created_at
FROM upload_errors ORDER BY created_at DESC;

-- View validation failures with count comparison
SELECT exporter, partition_value, pq_count, pg_count,
       (pq_count - pg_count) as diff, error_details
FROM validation_errors ORDER BY created_at DESC;

-- Find exporters with failures
SELECT exporter, COUNT(*) as failures
FROM upload_errors GROUP BY exporter;

-- Find partitions that failed but were later successfully uploaded
SELECT ue.exporter, ue.partition_value, ue.created_at as failed_at, u.uploaded_at as success_at
FROM upload_errors ue
JOIN uploads u ON ue.exporter = u.exporter AND ue.partition_value = u.partition_value
ORDER BY ue.created_at DESC;

-- Check total progress: uploaded vs total available per exporter
SELECT exporter, MIN(partition_value) as earliest, MAX(partition_value) as latest, COUNT(*) as uploaded
FROM uploads GROUP BY exporter ORDER BY exporter;

-- Find gaps in daily partitions for an exporter (dates present in errors but not in uploads)
SELECT ve.partition_value
FROM validation_errors ve
WHERE ve.exporter = 'block'
  AND ve.partition_value NOT IN (SELECT partition_value FROM uploads WHERE exporter = 'block')
ORDER BY ve.partition_value;
```

### Logging

Logs are written to both stderr and a timestamped file in `logs/`:

```
logs/yaci-s3-2024-01-15_143022.log
```

Each pipeline step is timed and logged with the format:
```
[exporter_name] Step: details (duration)
```

The file log always captures DEBUG level regardless of `--verbose`.

## Development

```bash
uv sync
uv run pytest tests/ -v
```

## Project Structure

```
src/yaci_s3/
    cli.py              # Click CLI entry point
    config.py            # .env + exporters.json loader
    models.py            # Dataclasses (ExporterDef, PartitionInfo, etc.)
    db.py                # SQLite tracking (uploads, errors, external state)
    scanner.py           # Local Hive-style partition discovery
    validator.py         # DuckDB + PG validation
    uploader.py          # S3 upload with retry + verification
    orchestrator.py      # Main pipeline + retry + external runner
    logging_setup.py     # Structured logging to stderr + file
    external/
        __init__.py          # External exporter registry
        base.py              # ExternalExporter ABC (fetch -> write -> upload)
        asset_data.py        # Minswap API client + exporter
        contract_registry.py # GitHub client + parsers + incremental exporter
```

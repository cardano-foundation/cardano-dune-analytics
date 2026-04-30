# yaci-s3

S3 upload tool for yaci-store Cardano parquet exports. Validates parquet files against PostgreSQL and uploads to S3 with SQLite-based tracking.

## Setup

```bash
# Install uv: https://docs.astral.sh/uv/getting-started/installation/
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
cp .env.example .env  # then edit with real values
```

For deploying to a new production server (with cron + Telegram), see [Deployment](#deployment).

## Configuration

### `.env`

| Variable | Description | Default |
|----------|-------------|---------|
| `PG_HOST` | PostgreSQL host | (required, unless `SKIP_PG_VALIDATION=true`) |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_DB` | PostgreSQL database | (required, unless `SKIP_PG_VALIDATION=true`) |
| `PG_USER` | PostgreSQL user | (required, unless `SKIP_PG_VALIDATION=true`) |
| `PG_PASSWORD` | PostgreSQL password | (required, unless `SKIP_PG_VALIDATION=true`) |
| `PG_SCHEMA` | PostgreSQL schema | `public` |
| `S3_BUCKET` | S3 bucket name | (required) |
| `AWS_PROFILE` | AWS CLI profile name for credentials | (optional) |
| `BASE_DATA_PATH` | Local path to **read** source parquet exports (yaci-store output) | (required) |
| `EXPORT_DATA_PATH` | Local path to **write** external/hybrid exporter output. Falls back to `BASE_DATA_PATH` if unset. Set when source dir is read-only. | `BASE_DATA_PATH` |
| `SQLITE_PATH` | Path to tracking database | `./uploads.db` |
| `BLOCKFROST_PROJECT_ID` | Blockfrost API project ID for DRep metadata resolution | (required for drep_profile) |
| `ANCHOR_MAX_WORKERS` | Max parallel threads for DRep metadata resolution | `5` |
| `SKIP_PG_VALIDATION` | When `true`, the daily pipeline script passes `--skip-validation` to dune exporters. Use on servers without PG access. | `false` |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token for pipeline notifications | (optional) |
| `TELEGRAM_CHAT_ID` | Telegram chat ID for pipeline notifications | (optional) |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL for pipeline notifications | (optional) |

### `exporters.json`

Defines the 13 dune-group exporters (9 daily, 4 epoch — `block`, `transaction`, `transaction_metadata`, `transaction_scripts`, `tx_input`, `assets`, `address_utxo`, `voting_procedure`, `gov_action_proposal`, `reward`, `epoch_stake`, `gov_action_proposal_status`, `adapot`). Each entry specifies the exporter name, PG table, slot/epoch column, and partition type.

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
| `asset_data` | Minswap API | Every 2 hours | Full snapshot of all verified Cardano tokens with price data. Fetched_at column is timestamp; meme tokens with `Stablecoin` category are filtered out; ticker falls back to `name` for bridged tokens. |
| `smart_contract_registry` | GitHub repos | Daily | Incremental export of new smart contract script hashes |
| `off_chain_pool_data` | Blockfrost API | Daily | Pool off-chain metadata (ticker, name, homepage). Supports `--rebuild` for full resolution. |

```bash
# Run a single external exporter
uv run yaci-s3 --external asset_data

# Run all external exporters
uv run yaci-s3 --external-all

# Dry run (fetch + write parquet locally, skip S3 upload)
uv run yaci-s3 --external asset_data --dry-run

# Verbose logging
uv run yaci-s3 --external smart_contract_registry --verbose
```

External exporters **do not** require PostgreSQL credentials — only `S3_BUCKET` and `BASE_DATA_PATH` are needed.

### DRep Profile (Internal Job)

The DRep profile builder creates a persistent lookup table at `{BASE_DATA_PATH}/drep_profile/drep_profile.parquet` containing one row per `drep_id` with resolved metadata (name, anchor URL) via the Blockfrost API. This is used by the `drep_dist_enriched` hybrid exporter.

**Profile schema:**

| Column | Type | Description |
|--------|------|-------------|
| `drep_id` | string | DRep identifier (primary key) |
| `drep_hash` | string | DRep credential hash |
| `anchor_url` | string | Working anchor URL that resolved successfully |
| `anchor_hash` | string | Hash of the anchor content |
| `drep_name` | string | `body.givenName` from anchor JSON |
| `source_block` | int64 | Block height of the registration row used |
| `source_slot` | int64 | Slot of the registration row used |
| `source_tx_hash` | string | Tx hash of the registration row used |
| `source_date` | string | Date partition the registration came from |
| `fetch_status` | string | `success`, `failed`, `no_url`, or `pending` |
| `http_status` | int32 | HTTP status code from the fetch |
| `last_checked_at` | string | ISO timestamp of last resolution attempt |
| `updated_at` | string | ISO timestamp of last profile update |

**Update logic:**
- For each `drep_id`, queries Blockfrost's `/governance/dreps/{drep_id}/metadata` endpoint which returns the resolved `givenName`
- Never overwrites a good profile with a failed resolution — keeps the existing name if new resolution fails
- New dreps with no anchor URL get `fetch_status=no_url`

```bash
# Rebuild from scratch (all historical drep_registration files)
uv run yaci-s3 --internal drep_profile --rebuild

# Update from a specific day's registration data
uv run yaci-s3 --internal drep_profile --date 2024-01-15

# Update from a date range
uv run yaci-s3 --internal drep_profile --start-date 2024-01-01 --end-date 2024-01-31

# Dry run (show what would change, don't write)
uv run yaci-s3 --internal drep_profile --rebuild --dry-run
```

### Off-Chain Pool Data (External Exporter)

Fetches pool off-chain metadata (ticker, name, description, homepage) from the Blockfrost API and exports as date-partitioned parquet files with S3 upload.

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `pool_hash` | string | Pool hash hex (primary key) |
| `pool_id` | string | Bech32 pool ID from Blockfrost |
| `ticker` | string | Pool ticker (e.g. `TAPSY`) |
| `name` | string | Pool display name |
| `description` | string | Pool description |
| `homepage` | string | Pool homepage URL |
| `metadata_url` | string | URL of the off-chain metadata JSON |
| `metadata_hash` | string | Hash of the metadata content |
| `fetched_at` | timestamp(us, tz=UTC) | When metadata was fetched |
| `version` | int32 | Row version number (1 = first appearance, increments on updates) |

**Two modes:**

- **Rebuild** (`--rebuild`): Resolves ALL unique pools from on-chain `pool_registration` parquet data. All rows get `version=1`.
- **Incremental** (default): Only resolves new and updated pools by comparing `pool_registration` date partitions against the last export date
  - **New pools**: pool_hash not in any previous export — gets `version=1`
  - **Updated pools**: pool_hash appears in pool partitions after the last export date (re-registrations) — gets `version=max(existing)+1`

Only successfully resolved pools are included — failed resolutions are skipped.

```bash
# Initial build — resolve all pools
uv run yaci-s3 --external off_chain_pool_data --rebuild

# Daily incremental — new + updated pools only
uv run yaci-s3 --external off_chain_pool_data

# Write parquet locally without uploading to S3
uv run yaci-s3 --external off_chain_pool_data --dry-run
```

**S3 path:** `off_chain_pool_data/{YYYY-MM-DD}/off_chain_pool_data-{YYYY-MM-DD}.{N}.parquet`

### Smart Contract Registry (External Exporter)

Fetches smart contract script hash data from GitHub repositories and exports as date-partitioned parquet files. Uses the GitHub Compare API to only fetch changed files on incremental runs.

**Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `script_hash` | string | Contract script hash (primary key) |
| `project_name` | string | Project name from the registry |
| `contract_name` | string | Individual contract/script name |
| `category` | string | Project category (e.g. DEX, DeFi) |
| `sub_category` | string | Project subcategory |
| `purpose` | string | Script purpose (e.g. spend) |
| `language` | string | Contract language (e.g. Plutus, PlutusV2) |
| `fetched_at` | timestamp(us, tz=UTC) | When data was fetched |
| `version` | int32 | Row version number (1 = first appearance, increments on updates) |

**Sources** (checked in priority order — highest priority wins when same script_hash appears in multiple):
1. **StricaHQ** — `StricaHQ/cardano-contracts-registry` (master, `projects/`)
2. **CRFA v2** — `mezuny/crfa-offchain-data-registry` (main, `dApps_v2/`)
3. **CRFA v1** — `mezuny/crfa-offchain-data-registry` (main, `dApps/`)

**Versioning:** On each export, the exporter reads existing parquet files to find the max version per script_hash. New hashes get `version=1`. Re-exported hashes (metadata updates) get `version=max(existing)+1`.

```bash
# Daily incremental (only changed files since last commit)
uv run yaci-s3 --external smart_contract_registry

# Dry run
uv run yaci-s3 --external smart_contract_registry --dry-run
```

**S3 path:** `smart_contract_registry/{YYYY-MM-DD}/smart_contract_registry-{YYYY-MM-DD}.parquet`

### Hybrid Exporters

Hybrid exporters join local parquet data sources to produce enriched output and upload to S3. They scan source exporter partitions, skip already-uploaded ones, enrich via joins, and upload the result.

**Available hybrid exporters:**

| Exporter | Source | Join With | Added Columns |
|----------|--------|-----------|---------------|
| `drep_dist_enriched` | `drep_dist` (epoch) | `drep_profile.parquet` | `anchor_url`, `drep_name` |

**Output S3 path:** `drep_dist_enriched/{epoch}/drep_dist_enriched-epoch-{epoch}.parquet`

```bash
# Enrich all new drep_dist epochs and upload
uv run yaci-s3 --hybrid drep_dist_enriched

# Enrich a specific epoch
uv run yaci-s3 --hybrid drep_dist_enriched --partition 611

# Run all hybrid exporters
uv run yaci-s3 --hybrid-all

# Dry run (enrich + write locally, skip S3 upload)
uv run yaci-s3 --hybrid drep_dist_enriched --dry-run
```

Hybrid exporters **do not** require PostgreSQL credentials.

#### Additional Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GITHUB_TOKEN` | GitHub personal access token (raises rate limit from 60 to 5000 req/hour) | (optional) |
| `MINSWAP_REQUEST_DELAY` | Delay in seconds between Minswap API pages | `5.0` |
| `MINSWAP_MAX_RETRIES` | Max retries for failed Minswap API requests | `5` |

#### File Naming

Most external exporters produce one file per day:

- **S3 key**: `{exporter}/{YYYY-MM-DD}/{exporter}-{YYYY-MM-DD}.parquet`
- **Local**: `{EXPORT_DATA_PATH}/{exporter}/date={YYYY-MM-DD}/{exporter}-{YYYY-MM-DD}.parquet` (falls back to `BASE_DATA_PATH` if `EXPORT_DATA_PATH` is not set)

`asset_data` runs every 2 hours and uses a `.N` suffix for multiple snapshots per day:

- **S3 key**: `asset_data/{YYYY-MM-DD}/asset_data-{YYYY-MM-DD}.{N}.parquet`
- **Tracking DB partition_value**: `{YYYY-MM-DD}.{N}` (e.g., `2024-01-15.1`, `2024-01-15.2`)

#### Scheduling

For production scheduling, see the [Deployment](#deployment) section. Production uses:

- `scripts/daily-pipeline.sh` for the full chain (dune exporters → drep_profile → off_chain_pool_data → drep_dist_enriched → CloudFront manifest), with retry polling
- `scripts/run-exporter.sh` for one-off cron jobs (`asset_data` every 2h, `smart_contract_registry` daily)
- `scripts/crontab` is the canonical schedule template — install with `crontab scripts/crontab` after editing `PROJ_DIR`

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
| `--external NAME` | Run a single external exporter (`asset_data`, `smart_contract_registry`, or `off_chain_pool_data`) |
| `--external-all` | Run all external exporters |
| `--hybrid NAME` | Run a single hybrid exporter (e.g. `drep_dist_enriched`) |
| `--hybrid-all` | Run all hybrid exporters |
| `--internal NAME` | Run an internal job (e.g. `drep_profile`) |
| `--rebuild` | Rebuild from scratch (for internal jobs and `off_chain_pool_data`) |
| `--date YYYY-MM-DD` | Process a specific date (for internal jobs) |
| `--start-date YYYY-MM-DD` | Start date for internal job date range |
| `--end-date YYYY-MM-DD` | End date for internal job date range |
| `--validate-adapot` | Validate adapot data against Koios `/totals` API (treasury, reserves, fees, deposits_stake, pool_rewards ratio) |
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
| Build DRep profile from scratch | `uv run yaci-s3 --internal drep_profile --rebuild` |
| Update DRep profile for one day | `uv run yaci-s3 --internal drep_profile --date 2024-01-15` |
| Update DRep profile for date range | `uv run yaci-s3 --internal drep_profile --start-date 2024-01-01 --end-date 2024-01-31` |
| Dry run DRep profile rebuild | `uv run yaci-s3 --internal drep_profile --rebuild --dry-run` |
| Build pool metadata from scratch | `uv run yaci-s3 --external off_chain_pool_data --rebuild` |
| Incremental pool metadata (new + updated) | `uv run yaci-s3 --external off_chain_pool_data` |
| Dry run pool metadata (write locally, no S3) | `uv run yaci-s3 --external off_chain_pool_data --dry-run` |
| Enrich all new drep_dist epochs | `uv run yaci-s3 --hybrid drep_dist_enriched` |
| Enrich a specific epoch | `uv run yaci-s3 --hybrid drep_dist_enriched --partition 611` |
| Run all hybrid exporters | `uv run yaci-s3 --hybrid-all` |
| Dry run hybrid exporter | `uv run yaci-s3 --hybrid drep_dist_enriched --dry-run` |

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
- External: `{exporter}/{YYYY-MM-DD}/{exporter}-{YYYY-MM-DD}.parquet` (or `.{N}.parquet` for `asset_data`)
- Hybrid: `{exporter}/{N}/{exporter}-epoch-{N}.parquet`

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
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    exporter              TEXT NOT NULL,           -- e.g. "asset_data", "smart_contract_registry"
    run_started_at        TEXT NOT NULL,           -- ISO 8601 UTC timestamp
    run_completed_at      TEXT,                    -- ISO 8601 UTC timestamp
    records_fetched       INTEGER,                 -- rows fetched from API
    records_exported      INTEGER,                 -- rows written to parquet
    status                TEXT NOT NULL DEFAULT 'running',  -- running, completed, failed
    error_details         TEXT,                    -- error message if failed
    source_data_watermark TEXT                     -- max source partition consumed (e.g. pool_registration date or drep_dist epoch); used to drive incremental runs
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

## Deployment

Production runs on `ssh dune` (Debian 13). Yaci-store data is sshfs-mounted from another node at `/opt/yaci-store/analytics/` (read-only). External / hybrid exporter output is written to `~/cardano-dune-analytics/exports/` via `EXPORT_DATA_PATH`. The dune host has no PostgreSQL access, so `SKIP_PG_VALIDATION=true` is set in `.env` (validation is skipped for the dune-group exporters; row counts and slot ranges are still recorded).

### Bootstrapping a new server

These steps assume Debian/Ubuntu with Python 3.12+ and `curl`/`wget`. If `git` isn't installed, use `wget` + tarball (see `scripts/update-code.sh`).

1. **Install uv**:
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```
2. **Get the code** (with git, or via the tarball helper if git is unavailable):
    ```bash
    git clone https://github.com/cardano-foundation/cardano-dune-analytics.git ~/cardano-dune-analytics
    # or:
    wget -O /tmp/repo.tar.gz https://github.com/cardano-foundation/cardano-dune-analytics/archive/refs/heads/master.tar.gz
    mkdir -p ~/cardano-dune-analytics && tar -xzf /tmp/repo.tar.gz -C /tmp \
      && cp -r /tmp/cardano-dune-analytics-master/. ~/cardano-dune-analytics/ \
      && chmod +x ~/cardano-dune-analytics/scripts/*.sh
    ```
3. **Install AWS credentials** at `~/.aws/credentials` and `~/.aws/config` (referenced via `AWS_PROFILE` in `.env`). The pipeline uses **boto3 only** — the `aws` CLI is not required.
4. **Create `.env`** with the variables above. On servers without PG access (e.g., dune), set `SKIP_PG_VALIDATION=true` and leave PG_* values dummy.
5. **Install dependencies**:
    ```bash
    cd ~/cardano-dune-analytics && uv sync
    ```
6. **Seed `uploads.db` and `exports/`** (optional, but avoids rediscovery / re-upload of historical data) by copying from a previous host or running `--rebuild-tracking`:
    ```bash
    # From an existing host:
    scp old-host:~/cardano-dune-analytics/uploads.db ~/cardano-dune-analytics/
    scp -r old-host:~/cardano-dune-analytics/exports ~/cardano-dune-analytics/
    # Or rebuild tracking from S3:
    uv run yaci-s3 --rebuild-tracking
    ```
7. **Test the pipeline**:
    ```bash
    ~/cardano-dune-analytics/scripts/daily-pipeline.sh
    ```
8. **Install the crontab** (edit `PROJ_DIR` first):
    ```bash
    vi ~/cardano-dune-analytics/scripts/crontab   # set PROJ_DIR
    crontab ~/cardano-dune-analytics/scripts/crontab
    crontab -l   # verify
    ```

### Cron schedule

Defined in `scripts/crontab`. Three jobs:

| Time (UTC) | Job | Notes |
|---|---|---|
| 02:00 daily | `daily-pipeline.sh` | All 5 steps with retry polling. See below. |
| 06:00 daily | `--external smart_contract_registry` | Independent; checks GitHub for updates. |
| Every 2h | `--external asset_data` | Independent; fetches Minswap API. |

### Daily pipeline (`daily-pipeline.sh`)

Sequential, with **retry polling** for step 1 and a Telegram/Slack notification at the end. Yaci-store on the source node writes the previous day's parquet around 02:00 UTC; the pipeline starts at 02:00 UTC and retries every 10 minutes (max 2 hours) until step 1 finds new partitions.

1. **Dune exporters** (13) — `--dune --parallel 4` (`--skip-validation` if `SKIP_PG_VALIDATION=true`)
2. **DRep profile** — `--internal drep_profile --date {yesterday}`
3. **Off-chain pool data** — `--external off_chain_pool_data` (incremental, watermark-driven)
4. **DRep dist enriched** — `--hybrid drep_dist_enriched`
5. **CloudFront manifest** — `update-manifest.sh` (lists S3 parquet files, writes `cloudfront_manifest.json`)

### Notifications

Set `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` and/or `SLACK_WEBHOOK_URL` in `.env`. The script sends a summary message at the end of each run, and an error message if any step fails (the failing step name is included).

### CloudFront manifest

`update-manifest.sh` lists every parquet file in the S3 bucket and writes a JSON manifest with public URLs. The manifest is uploaded to `s3://{S3_BUCKET}/cloudfront_manifest.json` and is publicly accessible at `https://d1gyygzinbyryv.cloudfront.net/cloudfront_manifest.json`.

### Updating code on a server without git

Use `scripts/update-code.sh` to pull the latest `master` tarball from GitHub, replacing only source/test/script files. `.env`, `uploads.db`, and `exports/` are preserved.

```bash
~/cardano-dune-analytics/scripts/update-code.sh
uv sync   # only if dependencies changed
```

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
        __init__.py              # External exporter registry
        base.py                  # ExternalExporter ABC (fetch -> write -> upload)
        asset_data.py            # Minswap API client + exporter
        contract_registry.py     # GitHub client + parsers + incremental exporter (smart_contract_registry)
        off_chain_pool_data.py   # Blockfrost pool metadata (rebuild + incremental)
    internal/
        __init__.py          # Internal job registry
        anchor_resolver.py   # Blockfrost API client (DRep + pool metadata, rate limiter)
        drep_profile.py      # DRep profile builder (DuckDB + Blockfrost resolution)
    hybrid/
        __init__.py          # Hybrid exporter registry
        base.py              # HybridExporter ABC (scan + enrich + upload)
        drep_dist_enriched.py # Join drep_dist with drep_profile
    validators/
        __init__.py
        adapot_koios.py     # Validate adapot supply data against Koios /totals
scripts/
    daily-pipeline.sh   # Cron entry point: dune (with retry polling) -> drep_profile -> off_chain_pool_data -> drep_dist_enriched -> manifest
    run-exporter.sh     # Wrapper for one-off cron jobs (asset_data, smart_contract_registry)
    update-manifest.sh  # Generate cloudfront_manifest.json (boto3, no aws CLI required)
    update-code.sh      # Pull latest code via wget tarball (used on servers without git)
    crontab             # Cron schedule template (edit PROJ_DIR before installing)
tests/                  # pytest test suite
```

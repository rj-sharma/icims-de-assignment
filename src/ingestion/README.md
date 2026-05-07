# Source Ingestion Design

This document explains the raw ingestion layer for the assignment. The local implementation is intentionally lightweight, but the design mirrors real-world data engineering patterns: each source is treated according to its expected arrival pattern, load frequency, and data semantics.

## Purpose Of The Raw Layer

The raw layer is the first persisted copy of source data inside the analytical system.

It should:

- Preserve source values with minimal transformation.
- Add ingestion metadata for lineage and debugging.
- Support reruns without silently duplicating data.
- Keep enough audit information to answer: what file/source was loaded, when, how many rows, and whether it succeeded.
- Defer business cleanup to dbt staging models.

## Local Technology Choice

For this assignment:

- Python reads CSV, JSON, and JSONL files.
- DuckDB stores the raw tables locally.
- dbt reads the raw tables and builds staging/mart models.

By default, ingestion writes to `icims.duckdb`. For testing or when the main DuckDB file is open in another tool, the path can be overridden:

```bash
ICIMS_DB_PATH=/tmp/icims_test.duckdb python3 src/ingestion/create_tables.py
ICIMS_DB_PATH=/tmp/icims_test.duckdb python3 src/ingestion/load_data.py
```

In production, the same pattern would usually become:

```text
Source systems
  -> S3 landing zone
  -> Spark/Glue/EMR ingestion
  -> Bronze Iceberg tables
  -> dbt/Spark transformations
```

## Code Layout

The ingestion code is split by responsibility:

| File | Responsibility |
| --- | --- |
| `create_tables.py` | Recreates local raw DuckDB tables for a clean assignment run |
| `load_data.py` | Ingestion orchestrator that calls each source loader |
| `common.py` | Shared connection, checksum, hash, path, and audit helpers |
| `ingest_applications.py` | Daily application extract append with latest-state staging |
| `ingest_education.py` | Weekly/monthly education batch replace |
| `ingest_candidates.py` | Candidate profile snapshot append with PII hash columns |
| `ingest_jobs.py` | Job requisition snapshot append with latest-state staging |
| `ingest_workflow_events.py` | Workflow JSONL event load |

## Source Assumptions

| Source file | Assumed source type | Expected frequency | Local strategy | Production strategy |
| --- | --- | --- | --- | --- |
| `education.csv` | Periodic batch file | Weekly/monthly | Batch replace by deterministic file batch | S3 landing + checksum + Iceberg overwrite/MERGE |
| `workflow_events.jsonl` | Event stream/export | Hourly/daily | Append-only event batch with deterministic event identity | Append-only partitioned event table |
| `candidates.json` | Candidate profile source | Daily snapshot or CDC | Append raw snapshot with email/phone hashes | CDC MERGE, tokenization, masking, or SCD Type 2 |
| `jobs.csv` | Job requisition source | Daily snapshot or CDC | Append raw snapshot, latest-state in staging | Latest-state silver table or SCD Type 2 |
| `applications.csv` | Application source | Daily incremental/snapshot | Append raw extract, latest-state in staging | Append bronze + MERGE current silver by `application_id` |

The first sources converted to more production-like patterns are `education.csv`, `applications.csv`, `workflow_events.jsonl`, and `jobs.csv`.

## Current Raw Audit Columns

Most raw tables include:

- `_ingestion_ts`: timestamp when the file was loaded
- `_batch_id`: identifier for the ingestion batch
- `_file_name`: source file name

`raw.education`, `raw.applications`, `raw.workflow_events`, `raw.jobs`, and `raw.candidates` have additional lineage columns:

- `_ingestion_date`: date derived from `_ingestion_ts`, used for daily processing filters
- `_source_system`: logical source identifier
- `_source_file_checksum`: MD5 checksum of the source file
- `_record_hash`: MD5 hash of the row's business columns

`raw.workflow_events` also stores:

- `_event_id`: deterministic event identity from application/status/timestamp fields
- `_event_date`: date derived from `event_timestamp`, useful as a logical partition key

`raw.candidates` also stores:

- `email_hash`: deterministic one-way hash of normalized email
- `phone_hash`: deterministic one-way hash of normalized phone

These columns help support idempotency, observability, and downstream debugging.

## Batch Audit Table

`raw.ingestion_batches` records source-level batch metadata.

Columns:

| Column | Meaning |
| --- | --- |
| `batch_id` | Deterministic or generated load identifier |
| `source_name` | Logical source, for example `education` |
| `source_system` | Source system or ingestion mechanism |
| `file_name` | Loaded file name |
| `file_checksum` | File checksum used to identify reruns |
| `load_strategy` | Pattern such as `batch_replace`, `append`, or `merge` |
| `status` | `STARTED`, `SUCCEEDED`, or `FAILED` |
| `started_at` | Load start timestamp |
| `completed_at` | Load end timestamp |
| `rows_loaded` | Number of rows inserted |
| `error_message` | Error details when a load fails |

This is a small local version of a production ingestion audit table.

## Implemented Pattern: `education.csv`

### Why This Pattern

I assume `education.csv` is a weekly or monthly batch file. It represents a periodic extract of candidate education records.

For this kind of source, a rerun of the same file should not append duplicate rows. Instead, the pipeline should recognize the same file and replace the previously loaded copy of that batch.

### Local Implementation

`ingest_education.py` does the following:

1. Reads `data/education.csv`.
2. Calculates an MD5 checksum for the file.
3. Builds a deterministic `batch_id` from source name, file name, and checksum.
4. Adds ingestion metadata and row-level `_record_hash`.
5. Deletes existing raw rows for the same `_batch_id`.
6. Deletes the old audit row for the same `batch_id`.
7. Inserts a new `STARTED` record into `raw.ingestion_batches`.
8. Inserts rows into `raw.education`.
9. Updates the audit row to `SUCCEEDED` with `rows_loaded`.
10. If an error occurs, marks the audit row as `FAILED`.

The important behavior: running the same education file multiple times keeps `raw.education` at 2000 rows instead of appending duplicates.

### Staging Behavior

`stg_educations.sql`:

- Filters the source by `_ingestion_date = var("run_date")`.
- Deduplicates by `candidate_id`.
- Normalizes `degree` to uppercase.
- Trims `institution`.
- Carries lineage fields into staging.

This means staging reads only the intended processing window while keeping source lineage visible.

## Implemented Pattern: `applications.csv`

### Why This Pattern

I assume `applications.csv` is a daily extract from an application tracking system. It may be either:

- a daily incremental extract containing new or changed applications, or
- a daily snapshot where the same `application_id` can appear again with updated attributes.

For this kind of source, `application_id` is the natural business key. Raw ingestion should still preserve received extracts, while staging/silver should choose the latest clean row per `application_id`.

### Local Implementation

`ingest_applications.py` does the following:

1. Reads `data/applications.csv`.
2. Calculates an MD5 checksum for the file.
3. Builds a deterministic `batch_id` from source name, file name, and checksum.
4. Adds ingestion metadata and row-level `_record_hash`.
5. Checks `raw.ingestion_batches` to see whether this exact file batch already loaded successfully.
6. If the batch already loaded, skips the file.
7. If it is a new batch, inserts a new `STARTED` record into `raw.ingestion_batches`.
8. Appends the application extract into `raw.applications`.
9. Updates the audit row to `SUCCEEDED` with `rows_loaded`.
10. If an error occurs, marks the audit row as `FAILED`.

This is a local DuckDB equivalent of an append-only bronze extract table.

The important behavior: running the same applications file multiple times keeps `raw.applications` at 5000 rows for that unchanged extract. A corrected or changed extract would have a different checksum and would be appended as a new raw batch.

In a production lakehouse, I would keep immutable application extracts in bronze and expose a separate current-state silver table using `MERGE` or a window function over `application_id`.

### Staging Behavior

`stg_applications.sql`:

- Filters the source by `_ingestion_date = var("run_date")`.
- Deduplicates by `application_id`.
- Parses mixed-format `apply_date`.
- Carries lineage fields into staging.

The final mart uses `application_id` as the grain of `fct_applications`.

For the current assignment, `fct_applications` behaves as the business-level application fact. If application attributes were mutable in production, the raw extracts would remain append-only while a silver/current model would resolve the latest state before the fact is built.

## Implemented Pattern: `workflow_events.jsonl`

### Why This Pattern

I assume `workflow_events.jsonl` is an event stream export. In production this could arrive hourly, daily, or continuously from a queue or event bus.

Events are different from snapshot-style sources:

- We should not upsert by `application_id`, because one application has many lifecycle events.
- Raw event history should be append-oriented.
- Rerunning the same event file should not duplicate that same batch.
- Duplicate event identities across batches should be deduplicated in staging/silver.

### Local Implementation

`ingest_workflow_events.py` does the following:

1. Reads `data/workflow_events.jsonl`.
2. Calculates an MD5 checksum for the file.
3. Builds a deterministic `batch_id` from source name, file name, and checksum.
4. Generates `_event_id` from `application_id`, `old_status`, `new_status`, and `event_timestamp`.
5. Derives `_event_date` from `event_timestamp`.
6. Adds ingestion metadata and row-level `_record_hash`.
7. Deletes existing raw rows for the same `_batch_id` so file reruns are idempotent.
8. Inserts a new `STARTED` record into `raw.ingestion_batches`.
9. Appends the event batch into `raw.workflow_events`.
10. Updates the audit row to `SUCCEEDED` with `rows_loaded`.
11. If an error occurs, marks the audit row as `FAILED`.

This is a local DuckDB equivalent of landing an event batch into an append-only bronze table, while making the batch itself safely replayable.

The important behavior: running the same workflow file multiple times keeps `raw.workflow_events` at 16769 rows for that batch instead of duplicating the file contents.

### Staging Behavior

`stg_workflow_events.sql`:

- Filters the source by `_ingestion_date = var("run_date")`.
- Normalizes statuses to uppercase.
- Parses `event_timestamp` as a timestamp.
- Carries `event_date` as the logical event partition date.
- Deduplicates by deterministic `event_id`.
- Carries lineage fields into staging.

In production, `event_date` would be a strong partition candidate, while `application_id` and `event_timestamp` would be useful sort or clustering keys.

## Implemented Pattern: `jobs.csv`

### Why This Pattern

I assume `jobs.csv` is a daily job requisition snapshot or a CDC-style extract from a source recruiting system.

For the local assignment, the raw table stays append-oriented and staging exposes latest state by `job_id`:

- `raw.jobs` keeps each distinct snapshot batch.
- Rerunning the same exact file should not duplicate the batch.
- `stg_jobs` selects the latest clean row per `job_id`.
- If a job status changes from `Draft` to `Open` or `Open` to `Closed`, raw keeps both snapshots and staging/marts expose the latest or historical view.

For production analytics, there are two valid choices:

- SCD Type 1/latest-state if users only need current job attributes.
- SCD Type 2 if users need history, such as how long a job stayed open or when department/status changed.

### Local Implementation

`ingest_jobs.py` does the following:

1. Reads `data/jobs.csv`.
2. Calculates an MD5 checksum for the file.
3. Builds a deterministic `batch_id` from source name, file name, and checksum.
4. Adds ingestion metadata and row-level `_record_hash`.
5. Checks `raw.ingestion_batches` to see whether this exact file batch already loaded successfully.
6. If the batch already loaded, skips the file.
7. If it is a new batch, inserts a new `STARTED` record into `raw.ingestion_batches`.
8. Appends the job snapshot rows into `raw.jobs`.
9. Updates the audit row to `SUCCEEDED` with `rows_loaded`.
10. If an error occurs, marks the audit row as `FAILED`.

This is a local DuckDB equivalent of an append-only bronze snapshot table.

The important behavior: running the same jobs file multiple times keeps `raw.jobs` at 500 rows for that unchanged snapshot. A corrected or changed snapshot would have a different checksum and would be appended as a new raw batch.

### Staging Behavior

`stg_jobs.sql`:

- Filters the source by `_ingestion_date = var("run_date")`.
- Deduplicates by `job_id`.
- Trims `title`.
- Normalizes `department` and `status` to uppercase.
- Parses mixed-format `posted_date`.
- Carries lineage fields into staging.

The final `dim_job` currently behaves as SCD Type 1/latest state. In production, I would add a dbt snapshot or Iceberg MERGE process for SCD Type 2 if historical job attribute changes are required.

### Where SCD Type 2 Would Sit

SCD Type 2 should not be implemented in raw ingestion. Raw preserves snapshots as received. Staging standardizes and chooses the latest clean row per job for current-state processing.

Historical dimension logic belongs in the mart layer, for example:

```text
raw.jobs                  append-only snapshots
stg_jobs                  cleaned latest row per job_id
dim_job                   SCD Type 1 current dimension
dim_job_history/snapshot  SCD Type 2 history with effective dates
```

An SCD Type 2 `dim_job_history` would include:

- `job_id`
- `title`
- `department`
- `posted_date`
- `status`
- `effective_from`
- `effective_to`
- `is_current`
- `_record_hash`

## Implemented Pattern: `candidates.json`

### Why This Pattern

I assume `candidates.json` is a candidate profile snapshot or CDC-style extract from an applicant tracking system.

Candidate data includes PII such as email and phone. For the local assignment, I keep the original fields because the project needs to be easy to inspect. I also add deterministic hash columns to show how the pipeline could support privacy-aware matching and joins.

Important distinction:

- Hashing is one-way. You cannot unhash an email or phone number.
- If the business needs to retrieve the original value, use encryption or tokenization.
- If the business only needs matching/deduplication, deterministic hashing can be useful.

### Local Implementation

`ingest_candidates.py` does the following:

1. Reads `data/candidates.json`.
2. Converts the `skills` array into a comma-separated string for local DuckDB raw storage.
3. Calculates an MD5 checksum for the file.
4. Builds a deterministic `batch_id` from source name, file name, and checksum.
5. Adds `email_hash` and `phone_hash`.
6. Sorts skills before computing `_record_hash` so skill order changes do not create false profile changes.
7. Adds ingestion metadata and row-level `_record_hash`.
8. Checks `raw.ingestion_batches` to see whether this exact file batch already loaded successfully.
9. If the batch already loaded, skips the file.
10. If it is a new batch, inserts a new `STARTED` record into `raw.ingestion_batches`.
11. Appends the candidate snapshot into `raw.candidates`.
12. Updates the audit row to `SUCCEEDED` with `rows_loaded`.
13. If an error occurs, marks the audit row as `FAILED`.

The important behavior: running the same candidates file multiple times keeps `raw.candidates` at 2001 rows for that unchanged snapshot. A changed extract would have a different checksum and would be appended as a new raw batch.

### Staging Behavior

`stg_candidates.sql`:

- Filters the source by `_ingestion_date = var("run_date")`.
- Deduplicates by `candidate_id`.
- Trims names and phone.
- Lowercases email.
- Carries `email_hash` and `phone_hash`.
- Splits `skills` into `skills_array`.
- Sorts the skill list into a deterministic order.
- Adds `skills_normalized`, a stable comma-separated sorted skill string.
- Calculates `skills_count`.
- Carries lineage fields into staging.

### Production PII Handling

In production, I would not rely on simple unsalted MD5 hashing for sensitive data protection.

Better options:

- Encrypt raw PII at rest with KMS-managed keys.
- Use Lake Formation or warehouse permissions for column-level access.
- Mask email and phone in analyst-facing views.
- Use tokenization if downstream systems need reversible lookup.
- Use salted/HMAC hashing for deterministic matching when reversibility is not required.
- Store secrets/salts in a secrets manager, not in code.

Example production layout:

```text
raw.candidates_secure     encrypted source values, restricted access
silver.candidates_current cleaned current profile, limited access
gold.dim_candidate        masked or tokenized PII for analytics
```

## Why Not Clean Everything In Ingestion?

The raw layer should stay close to source data. It should not hide source quality issues.

Ingestion should do:

- File parsing
- Basic metadata addition
- Batch audit
- Idempotent load mechanics
- Minimal compatibility conversions, such as converting candidate skills from JSON arrays into a string for DuckDB

Staging should do:

- Type casting
- Date parsing
- Casing normalization
- Deduplication
- Business-key validation
- Data quality tests

This separation makes the pipeline easier to debug. If staging finds a bad date, we can trace it back to the exact raw batch and source file.

## Daily Processing Assumption

The local dbt pipeline accepts:

```bash
--vars '{"run_date":"YYYY-MM-DD"}'
```

For raw-to-staging, `run_date` represents the ingestion date, not necessarily the business event date.

This distinction matters:

- `_ingestion_date`: when the platform received the data
- `apply_date`: when a candidate applied
- `posted_date`: when a job was posted
- `event_timestamp`: when a workflow event happened

In production, late-arriving data is common. A workflow event from last week might arrive today. The pipeline should process it based on ingestion date, then update business metrics based on the affected application IDs.

## Production Optimization For Bigger Loads

### Storage

For large production loads, I would land files in S3 and store curated raw/bronze data as Iceberg tables backed by Parquet.

Benefits:

- Compressed columnar storage
- Partition pruning
- Schema evolution
- ACID writes
- MERGE support
- Time travel and rollback

### Batch File Optimization

For batch sources like `education.csv`:

- Land files at `s3://bucket/raw/education/ingestion_date=YYYY-MM-DD/`.
- Compute checksum before processing.
- Store file metadata in an ingestion audit table.
- Skip files already loaded successfully.
- For corrected files, load a new checksum/batch and expire or replace the previous batch.
- Write to Iceberg using overwrite-by-partition or MERGE.

### Event Stream Optimization

For large event sources like `workflow_events`:

- Keep raw events append-only.
- Partition by `event_date` or `ingestion_date`.
- Sort or cluster by `application_id` and `event_timestamp`.
- Generate deterministic `event_id`.
- Deduplicate in silver/staging using event identity.
- Process only new partitions.
- Recompute only impacted `application_id` values in gold facts.

### Snapshot / CDC Optimization

For application/profile-like sources such as applications, candidates, and jobs:

- If source provides full snapshots, compare `_record_hash` by business key to detect changed rows.
- If source provides CDC, apply inserts/updates/deletes with MERGE.
- For applications, merge by `application_id`.
- Use SCD Type 1 for latest-state dimensions when history is not needed.
- Use SCD Type 2 when historical reporting matters.

### Operational Optimizations

Production ingestion should also include:

- Batch audit table
- Row count checks
- File checksum validation
- Schema validation
- Quarantine table for malformed records
- CloudWatch or equivalent metrics
- Alerts for missing files, failed loads, row-count anomalies, and duplicate spikes

## Next Sources To Improve

Recommended next implementation order:

All provided sources now have an explicit local ingestion pattern. The next production-oriented improvements would be adding quarantine tables, stronger schema validation, and optional SCD Type 2 snapshots for dimensions that need history.

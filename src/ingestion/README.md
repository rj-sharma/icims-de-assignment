# Source Ingestion Design

This document explains the local raw ingestion layer. 
main README - [../../README.md](../../README.md). 
architecture README - [../../ARCHITECTURE.md](../../ARCHITECTURE.md).

## Purpose
The raw layer is the first persisted copy of source data in DuckDB.

It is responsible for:

- reading source files
- preserving source values with minimal business transformation
- adding audit and lineage metadata
- recording batch status in `raw.ingestion_batches`
- making reruns safe and deterministic
- leaving deeper cleanup to dbt staging models

## Local Stack
| Component | Tool |
| --- | --- |
| File reading | Python, pandas, JSON/JSONL parsing |
| Local database | DuckDB |
| Raw schema | `raw` |
| Orchestrator | `src/ingestion/load_data.py` |
| Source-specific loaders | `ingest_*.py` modules |


## Code Layout
| File | Responsibility |
| --- | --- |
| `create_tables.py` | Creates/recreates raw DuckDB tables |
| `load_data.py` | Calls all source-specific ingestion modules |
| `common.py` | Shared connection, checksum, batch ID, hash, audit helpers |
| `ingest_applications.py` | Loads application extracts |
| `ingest_candidates.py` | Loads candidate profile snapshots and hashes PII fields |
| `ingest_education.py` | Loads education batch files |
| `ingest_jobs.py` | Loads job requisition snapshots |
| `ingest_workflow_events.py` | Loads workflow event JSONL batches |

## Source Assumptions And Load Patterns

| Source | Assumption | Local raw pattern | Staging/mart behavior |
| --- | --- | --- | --- |
| `applications.csv` | Daily incremental extract or snapshot | Append only if file checksum has not already succeeded | `stg_applications` keeps latest row by `application_id`; `fct_applications` is one row per application |
| `candidates.json` | Daily candidate profile snapshot or CDC extract | Append only if file checksum has not already succeeded; add email/phone hashes | `stg_candidates` normalizes PII fields and sorted skill arrays; `dim_candidate` is current-state |
| `education.csv` | Weekly/monthly batch file | Replay-safe batch replace by deterministic `_batch_id` | `stg_educations` dedupes by `candidate_id`; joined into `dim_candidate` |
| `jobs.csv` | Daily job snapshot or CDC extract | Append only if file checksum has not already succeeded | `stg_jobs` keeps latest row by `job_id`; `dim_job` is SCD Type 1 current-state |
| `workflow_events.jsonl` | Hourly/daily event export | Replay-safe append batch with deterministic `_event_id` | `stg_workflow_events` dedupes by `event_id`; facts preserve event history |

## Audit Columns

Common raw metadata:

| Column | Purpose |
| --- | --- |
| `_ingestion_ts` | Timestamp when the local loader processed the file |
| `_ingestion_date` | Date derived from `_ingestion_ts`; used by dbt `run_date` filtering |
| `_batch_id` | Deterministic batch identifier |
| `_file_name` | Source file path/name |
| `_source_system` | Logical source label |
| `_source_file_checksum` | MD5 checksum of the source file |
| `_record_hash` | MD5 hash of business columns for change detection/debugging |

Additional source-specific metadata:

| Table | Extra columns |
| --- | --- |
| `raw.workflow_events` | `_event_id`, `_event_date` |
| `raw.candidates` | `email_hash`, `phone_hash` |

## Batch Audit Table

`raw.ingestion_batches` records source-level load status.

| Column | Meaning |
| --- | --- |
| `batch_id` | Deterministic batch identifier |
| `source_name` | Source table/domain, such as `workflow_events` |
| `source_system` | Logical source system |
| `file_name` | Loaded file |
| `file_checksum` | Checksum used to detect reruns |
| `load_strategy` | `append_snapshot`, `append_batch`, or `batch_replace` |
| `status` | `STARTED`, `SUCCEEDED`, or `FAILED` |
| `started_at` | Batch start timestamp |
| `completed_at` | Batch completion timestamp |
| `rows_loaded` | Number of inserted rows |
| `error_message` | Failure details |

## Idempotency Behavior

The ingestion layer supports reruns without duplicate raw rows:

- Applications, candidates, and jobs check whether the exact file checksum already loaded successfully. If yes, the loader skips the file.
- Education and workflow events delete rows for the same deterministic `_batch_id` before reloading. This makes file replay deterministic while keeping the source pattern batch-oriented.
- Workflow events generate deterministic `_event_id` from application/status/timestamp values. Staging deduplicates by this ID.
- dbt models use unique keys and deduplication windows downstream, so modeled tables also remain stable on rerun.

## Cleanup Boundary

Ingestion performs only light compatibility work:

- parse files
- add metadata
- compute hashes/checksums
- generate deterministic event IDs
- flatten candidate skills to a raw string for DuckDB storage

dbt staging performs business cleanup:

- trim strings
- normalize casing
- parse dates and timestamps
- deduplicate by business key
- split and sort candidate skills
- validate statuses and required fields

This keeps raw data traceable. If a staging test fails, the record can be traced back to the source file and batch.

## Candidate PII Handling

The local project keeps `email` and `phone` readable because the assignment data needs to be inspectable. It also stores deterministic hashes:

- `email_hash`
- `phone_hash`


In production, I would use:

- KMS encryption at rest
- Lake Formation or warehouse column-level permissions
- masked analyst-facing views
- HMAC/salted hashes for deterministic matching

## Daily Processing Semantics

The dbt pipeline accepts:

```bash
--vars "{run_date: 'YYYY-MM-DD'}"
```

`run_date` means ingestion date, not business event date.

Examples:

- `_ingestion_date`: when the platform received the file
- `apply_date`: when a candidate applied
- `posted_date`: when a job was posted
- `event_timestamp`: when a workflow transition happened

This matters for late-arriving data. A workflow event from last week may arrive today. The pipeline should process it based on today’s ingestion date, then update the impacted application timeline.

## Production Notes

For larger workloads, this ingestion design maps to:

```text
Source systems
  -> S3 landing zone
  -> Glue/EMR Spark ingestion
  -> Bronze Iceberg tables
  -> Silver cleaned tables
  -> dbt gold models
```

Key production upgrades:

- store raw files in immutable S3 prefixes
- use Iceberg/Parquet instead of local DuckDB raw tables
- partition events by `event_date` or `ingestion_date`
- sort/cluster workflow events by `application_id`, `event_timestamp`
- use `MERGE` for CDC/snapshot sources
- use quarantine tables for malformed records
- send row counts, duplicate counts, rejected rows, and anomaly counts to CloudWatch

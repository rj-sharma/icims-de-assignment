# Architecture

This document explains two versions of the same pipeline:

- Local assignment implementation: Python + DuckDB + dbt
- Production 10TB design: AWS lakehouse with Spark, Iceberg, dbt, Airflow, and governance

The local project is intentionally simple and runnable on a laptop. The production design shows how the same data model and business rules would scale when `workflow_events` becomes a 10TB dataset.

## Local Architecture

### Local Tech Stack

| Layer | Tool | Purpose |
| --- | --- | --- |
| Source storage | Local files under `data/` | Provided assignment datasets |
| Ingestion | Python + pandas + DuckDB | Read CSV/JSON/JSONL, add audit columns, write raw tables |
| Warehouse/query engine | DuckDB | Local analytical database |
| Transformation | dbt-duckdb | Staging, intermediate, marts, tests |
| Data quality | dbt tests + quality model | Uniqueness, not-null, freshness, volume, anomaly checks |
| Unit tests | pytest | Python ingestion helper tests |
| Orchestration | `scripts/run_pipeline.sh` | Local pipeline runner |

### Local Pipeline Diagram

```mermaid
flowchart LR
    subgraph Sources["Local Source Files"]
        A1[jobs.csv]
        A2[candidates.json]
        A3[education.csv]
        A4[applications.csv]
        A5[workflow_events.jsonl]
    end

    subgraph Python["Python Ingestion"]
        B1[create_tables.py]
        B2[load_data.py]
        B3[source-specific loaders]
    end

    subgraph Raw["DuckDB raw schema"]
        C1[raw.jobs]
        C2[raw.candidates]
        C3[raw.education]
        C4[raw.applications]
        C5[raw.workflow_events]
        C6[raw.ingestion_batches]
    end

    subgraph DBT["dbt Transformations"]
        D1[staging models<br/>clean, cast, dedupe]
        D2[int_workflow_events_enriched<br/>event sequence + anomaly flag]
        D3[marts<br/>facts, dimensions, aggregate]
        D4[quality models<br/>anomaly audit]
        D5[dbt tests<br/>freshness, uniqueness, volume]
    end

    subgraph Marts["DuckDB main_marts schema"]
        E1[dim_job]
        E2[dim_candidate]
        E3[fct_applications]
        E4[fct_workflow_events]
        E5[agg_time_to_hire_by_job_department]
    end

    A1 --> B3
    A2 --> B3
    A3 --> B3
    A4 --> B3
    A5 --> B3
    B1 --> C1
    B2 --> B3
    B3 --> C1
    B3 --> C2
    B3 --> C3
    B3 --> C4
    B3 --> C5
    B3 --> C6
    C1 --> D1
    C2 --> D1
    C3 --> D1
    C4 --> D1
    C5 --> D1
    D1 --> D2
    D1 --> D4
    D2 --> D3
    D1 --> D3
    D3 --> E1
    D3 --> E2
    D3 --> E3
    D3 --> E4
    D3 --> E5
    D1 --> D5
    D3 --> D5
```

### Local Modeling Notes

- Raw tables preserve source-shaped data and add `_ingestion_ts`, `_ingestion_date`, `_batch_id`, `_file_name`, file checksum, and record hash where applicable.
- Staging models clean values, parse dates, normalize statuses, and deduplicate by business key.
- `int_workflow_events_enriched` recomputes workflow event order for applications impacted by the current run.
- `fct_applications` calculates `hired_date`, `current_status`, `is_hired`, and `time_to_hire_days`.
- `agg_time_to_hire_by_job_department` is a simple full-table reporting aggregate for assignment readability.
- `dq_hired_before_applied_anomalies` persists the known bad records for audit.

## Production Architecture For 10TB Workflow Events

At 10TB, I would not parse JSONL with pandas or load it into one local DuckDB process. I would move ingestion and heavy transformation to a distributed AWS lakehouse while keeping the same logical layers: raw/bronze, cleaned/silver, and analytics/gold.

### Production Tech Stack

| Layer | Technology | Why |
| --- | --- | --- |
| Landing storage | Amazon S3 | Durable, low-cost storage for raw files and CDC extracts |
| Table format | Apache Iceberg on Parquet | ACID writes, schema evolution, partition pruning, time travel, MERGE |
| Distributed processing | AWS Glue Spark or EMR Spark | Parallel parsing and transformation for multi-TB files |
| Catalog | AWS Glue Data Catalog | Central metadata catalog for Iceberg tables |
| Schema registry | AWS Glue Schema Registry | Schema compatibility checks for event data |
| Governance | AWS Lake Formation | Table, column, and row-level permissions, especially for PII |
| Transformation | dbt on Spark/Athena/Trino | SQL modeling, tests, docs, lineage |
| Orchestration | Airflow / MWAA | Scheduling, retries, dependencies, backfills |
| Monitoring | CloudWatch | Job logs, row counts, failures, latency, alerts |
| Data quality | dbt tests plus Anomalo/Monte Carlo/Soda/Great Expectations/Deequ | Deterministic tests plus trend-based anomaly monitoring |
| Query layer | Athena, Trino, Spark SQL, Redshift Spectrum | SQL access to curated Iceberg/Parquet tables |

### Production Pipeline Diagram

```mermaid
flowchart LR
    subgraph Sources
        A1[Workflow Events<br/>10TB event files or stream]
        A2[Education CSV<br/>weekly/monthly batch]
        A3[Candidate SQL DB<br/>DMS CDC or incremental extract]
        A4[Jobs DB / Applications DB<br/>CDC or daily extracts]
    end

    subgraph Landing["S3 Landing Zone"]
        B1[s3://landing/workflow_events/<br/>ingestion_date=YYYY-MM-DD]
        B2[s3://landing/education/<br/>batch_date=YYYY-MM-DD]
        B3[s3://landing/candidates/<br/>extract_date=YYYY-MM-DD]
        B4[s3://landing/jobs_applications/<br/>extract_date=YYYY-MM-DD]
    end

    subgraph Control["Control And Governance"]
        C1[Airflow / MWAA]
        C2[Glue Schema Registry]
        C3[Glue Data Catalog]
        C4[Lake Formation]
        C5[CloudWatch]
    end

    subgraph Compute["Distributed Compute"]
        D1[Glue or EMR Spark<br/>Bronze ingestion]
        D2[Glue or EMR Spark<br/>Silver cleaning and dedupe]
        D3[dbt on Spark/Athena/Trino<br/>Gold marts]
    end

    subgraph Lakehouse["S3 Lakehouse With Iceberg Tables"]
        E1[Bronze<br/>raw structured tables]
        E2[Silver<br/>cleaned and deduplicated tables]
        E3[Gold<br/>facts, dimensions, aggregates]
        E4[Quarantine<br/>bad records and DQ failures]
    end

    subgraph Consumers
        F1[Athena / Trino SQL]
        F2[BI dashboards]
        F3[Data science]
        F4[Operational analytics]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
    A4 --> B4

    C1 --> D1
    C1 --> D2
    C1 --> D3
    C2 --> D1
    C3 --> E1
    C3 --> E2
    C3 --> E3
    C4 --> C3

    B1 --> D1
    B2 --> D1
    B3 --> D1
    B4 --> D1

    D1 --> E1
    E1 --> D2
    D2 --> E2
    D2 --> E4
    E2 --> D3
    D3 --> E3
    D3 --> E4

    D1 --> C5
    D2 --> C5
    D3 --> C5

    E3 --> F1
    E3 --> F2
    E2 --> F3
    E3 --> F4
```

### Production Orchestration Diagram

```mermaid
flowchart TD
    A[start] --> B[check_source_arrival]
    B --> C[validate_schema<br/>Glue Schema Registry]
    C --> D[bronze_workflow_events<br/>Spark to Iceberg]
    C --> E[bronze_education]
    C --> F[bronze_candidates]
    C --> G[bronze_jobs_applications]

    D --> H[silver_workflow_events<br/>parse, dedupe, flag anomalies]
    E --> I[silver_education]
    F --> J[silver_candidates<br/>PII controls]
    G --> K[silver_jobs_applications]

    H --> L[run_silver_quality_checks]
    I --> L
    J --> L
    K --> L

    L --> M[dbt build gold models]
    M --> N[dbt test gold models]
    N --> O[publish metrics]
    O --> P[end]

    L --> Q[quarantine bad records]
    N --> Q
    Q --> R[CloudWatch alerts]
```

## 10TB Workflow Event Strategy

### Storage Format

Use Apache Iceberg tables backed by Parquet files on S3.

Why:

- JSONL is expensive to parse repeatedly.
- Parquet is columnar and compressed.
- Iceberg supports ACID writes, `MERGE`, schema evolution, hidden partitioning, time travel, and rollback.
- Athena, Spark, Trino, and dbt-compatible engines can query the same open tables.

### Partitioning And Layout

For `workflow_events`, use:

```text
Partition: event_date or ingestion_date
Sort/cluster: application_id, event_timestamp
Target file size: 256MB to 1GB
```

Guidance:

- Use `event_date` when most queries filter by business event time.
- Use `ingestion_date` when late-arriving data and operational replay are more important.
- Do not partition by `application_id`; the cardinality is too high.
- Use compaction to avoid many small files.

### Incremental Processing

Bronze:

- Process new files only.
- Track file path, checksum, batch ID, status, row count, and rejection count.
- Skip already successful batches.
- Reprocess failed/corrected batches with overwrite-by-batch or Iceberg `MERGE`.

Silver:

- Read only new bronze partitions.
- Parse timestamps, normalize statuses, generate deterministic `event_id`.
- Deduplicate by `event_id`.
- Flag hired-before-applied and malformed records.
- Write bad records to quarantine.

Gold:

- Avoid full fact rebuilds.
- Identify impacted `application_id` values from new workflow events.
- Recompute only those application timelines and `time_to_hire_days`.
- `MERGE` impacted rows into `gold.fct_applications` and `gold.fct_workflow_events`.
- Recompute reporting aggregates by impacted job, department, or date partition.

## Data Quality And Anomaly Handling

Data quality should run across layers:

| Layer | Checks |
| --- | --- |
| Bronze | file exists, schema compatible, parseable, row count above zero |
| Silver | required IDs not null, timestamp parseable, accepted statuses, duplicate rate, quarantine malformed records |
| Gold | unique fact/dimension keys, referential integrity, non-negative Time to Hire, anomaly audit |

Hired-before-applied handling:

```text
Bronze: preserve source event
Silver: flag anomaly
Gold fact: exclude from hired_date and time_to_hire_days
Quality/audit: persist anomalous record and alert
```

This is safer than silently dropping the record because the source system may need investigation.

## Governance And PII

Candidate data contains email and phone values.

Production controls:

- Encrypt data at rest with KMS.
- Use Lake Formation for table, column, and row-level access.
- Restrict raw PII tables to data engineering and approved users.
- Expose masked/tokenized candidate views to analytics users.
- Use HMAC/salted hashes for deterministic matching when reversibility is not required.
- Use tokenization or encryption when authorized reverse lookup is required.

## Observability

CloudWatch and Airflow should track:

- source arrival time
- job duration
- rows read and written
- rejected rows
- duplicate rate
- anomaly count
- late-arriving event count
- Iceberg file counts and compaction status
- freshness and SLA misses

Recommended alerts:

- workflow events missing by expected time
- row count changes beyond threshold
- rejected records exceed threshold
- hired-before-applied anomalies spike
- Spark job duration increases sharply
- Iceberg compaction has not run recently

## Interview Summary

For the assignment, I used Python, DuckDB, and dbt because the data is small and the solution needs to be easy to run locally.

For a 10TB workflow event dataset, I would move to an AWS lakehouse. Raw files land in S3, Glue or EMR Spark reads them in parallel, validates schemas, and writes Parquet-backed Iceberg tables registered in Glue Data Catalog. Lake Formation governs access, especially for candidate PII. Airflow orchestrates ingestion, cleaning, dbt gold models, tests, and backfills. CloudWatch monitors job health, row counts, latency, rejected records, and anomalies.

The main scaling principle is to avoid full refreshes: process only new partitions, identify impacted `application_id` values, recompute those timelines, and merge the changed rows into gold facts and aggregates.

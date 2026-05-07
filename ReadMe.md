# iCIMS Data Engineering Assignment

This project implements a local data engineering pipeline for the iCIMS take-home assignment. It loads the provided recruiting datasets into DuckDB, transforms the raw data with dbt, creates analytical fact and dimension models, runs data quality checks, and answers the assignment SQL questions.

The implementation intentionally uses simple local tooling:

- Python for raw ingestion
- DuckDB as the local database and query layer
- dbt for staging, validation, transformations, marts, and tests

This keeps the project easy to run on a laptop while still showing production-oriented design choices such as raw metadata, deduplication, normalized staging models, business-rule tests, incremental models, and a scalable lakehouse design for larger data volumes.

## Project Structure

```text
.
├── analysis/
│   ├── source_data_analysis.md
│   └── source_data_analysis.ipynb
├── data/
│   ├── applications.csv
│   ├── candidates.json
│   ├── education.csv
│   ├── jobs.csv
│   └── workflow_events.jsonl
├── dbt/
│   └── icims_project/
│       ├── assignment_sql/
│       │   └── task1_result.sql
│       ├── macros/
│       │   └── parse_date.sql
│       ├── models/
│       │   ├── sources.yml
│       │   ├── staging/core/
│       │   ├── intermediate/
│       │   └── marts/
│       ├── tests/
│       └── dbt_project.yml
├── scripts/
│   ├── clean_local_artifacts.sh
│   └── run_pipeline.sh
├── src/
│   └── ingestion/
│       ├── README.md
│       ├── common.py
│       ├── create_tables.py
│       ├── ingest_applications.py
│       ├── ingest_candidates.py
│       ├── ingest_education.py
│       ├── ingest_jobs.py
│       ├── ingest_workflow_events.py
│       └── load_data.py
├── requirements.txt
├── architecture-readme.md
└── ReadMe.md
```

For the detailed AWS lakehouse scaling design for a 10TB `workflow_events` dataset, see `architecture-readme.md`.

For source-file profiling and first-pass data quality observations, see `analysis/source_data_analysis.md`. The same analysis is also available as a reproducible pandas notebook at `analysis/source_data_analysis.ipynb`.

For raw ingestion assumptions, audit columns, source-specific load patterns, see `src/ingestion/README.md`.

## Data Flow

```text
Source files
  -> Python ingestion
  -> DuckDB raw schema
  -> dbt staging models
  -> dbt intermediate models
  -> dbt marts
  -> SQL analysis and data quality tests
```

Local tables are created in `icims.duckdb`.

The raw layer preserves source-level records and adds operational metadata:

- `_ingestion_ts`
- `_batch_id`
- `_file_name`

The dbt layer then standardizes values, parses mixed date formats, deduplicates records, applies business logic, and creates analytical facts and dimensions.

## Prerequisites

- Python 3.10 or later
- `pip`
- Git or a zip extract of the project

The project uses `dbt-duckdb`. If dbt cannot find a profile, create `~/.dbt/profiles.yml`:

```yaml
icims_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: icims.duckdb
      threads: 4
```

## How To Run

### 1. Create And Activate Virtual Environment

Run all setup commands from the project root:

```bash
cd icims-de-assignment
```

Create a local Python virtual environment:

```bash
python3 -m venv venv
```

Activate it:

```bash
source venv/bin/activate
```

Install dependencies inside the virtual environment:

```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

Verify dbt is available:

```bash
dbt --version
```

If you open a new terminal later, activate the environment again before running the pipeline:

```bash
source venv/bin/activate
```

### 2. Optional Clean Local Artifacts

Use this when you want to remove generated files and test the project from a clean local state:

```bash
bash scripts/clean_local_artifacts.sh
```

The script removes generated artifacts such as local DuckDB files, dbt `target` directories, dbt packages, and logs. It does not remove source code or files under `data/`.

Preview what would be removed:

```bash
bash scripts/clean_local_artifacts.sh --dry-run
```

Also remove the local virtual environment:

```bash
bash scripts/clean_local_artifacts.sh --remove-venv
```

If you use `--remove-venv`, repeat step 1 before running Python or dbt commands again.

### 3. Create Raw Tables

```bash
python3 src/ingestion/create_tables.py
```

This creates the `raw` schema and raw tables in DuckDB.

### 4. Load Raw Data

```bash
python3 src/ingestion/load_data.py
```

Expected raw row counts after a clean load:


| Table                 | Expected rows |
| --------------------- | ------------- |
| `raw.jobs`            | 500           |
| `raw.candidates`      | 2001          |
| `raw.education`       | 2000          |
| `raw.applications`    | 5000          |
| `raw.workflow_events` | 16769         |


### 5. Run dbt Models

Use the date on which the ingestion ran. For example:

```bash
dbt run --project-dir dbt/icims_project --vars '{"run_date":"2026-05-05"}' --full-refresh
```

You can also run specifc layer model using tags

```bash
dbt run --project-dir dbt/icims_project --vars "{run_date: '2026-05-07'}" --select tag:stg --full-refresh
```

### 6. Run dbt Tests

```bash
dbt test --project-dir dbt/icims_project --vars '{"run_date":"2026-05-05"}'
```

To store failed records for debugging:

```bash
dbt test --project-dir dbt/icims_project --vars '{"run_date":"2026-05-05"}' --store-failures
```

### 7. Run The Pipeline Script

```bash
bash scripts/run_pipeline.sh --date 2026-05-05 --full
```

## Task Group 1: Data Ingestion And Basic Analysis

### Database Setup

`src/ingestion/create_tables.py` creates the raw DuckDB schema and source-aligned raw tables:

- `raw.jobs`
- `raw.candidates`
- `raw.education`
- `raw.applications`
- `raw.workflow_events`

Dates are initially stored as strings in raw tables so the ingestion layer does not reject dirty records too early. Date standardization happens in dbt using the reusable `parse_date` macro.

### Ingestion

`src/ingestion/load_data.py` orchestrates the source-specific ingestion modules:

- CSV: `jobs.csv`, `education.csv`, `applications.csv`
- JSON array: `candidates.json`
- JSONL event stream: `workflow_events.jsonl`

The ingestion step adds `_ingestion_ts`, `_batch_id`, and `_file_name` to support traceability. Candidate `skills` are converted from arrays to a comma-separated string to keep the local DuckDB model simple.

For this local assignment, a deterministic rerun is done by recreating the raw tables before ingestion. The dbt layer then deduplicates by business keys and uses incremental unique keys for modeled tables. In a production ingestion pipeline, I would make the raw loader fully idempotent by using deterministic file/batch IDs and deleting or merging a batch before reinserting it.

### SQL Analysis Answers

The SQL file is available at `dbt/icims_project/assignment_sql/task1_result.sql`.

#### 1. How many jobs are currently open?

```sql
SELECT COUNT(*) AS open_jobs
FROM icims.main_staging.stg_jobs
WHERE status = 'OPEN';
```

Answer:

```text
178
```

#### 2. Top 5 departments by number of applications

```sql
SELECT
    j.department,
    COUNT(a.application_id) AS total_applications
FROM icims.main_staging.stg_applications a
JOIN icims.main_staging.stg_jobs j
    ON a.job_id = j.job_id
GROUP BY j.department
ORDER BY total_applications DESC
LIMIT 5;
```

Answer:


| Department  | Applications |
| ----------- | ------------ |
| MARKETING   | 923          |
| PRODUCT     | 810          |
| ENGINEERING | 789          |
| SALES       | 761          |
| FINANCE     | 629          |


#### 3. Candidates who applied to more than 3 jobs

```sql
SELECT
    c.candidate_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(DISTINCT a.job_id) AS jobs_applied
FROM icims.main_staging.stg_applications a
JOIN icims.main_staging.stg_candidates c
    ON a.candidate_id = c.candidate_id
GROUP BY
    c.candidate_id,
    c.first_name,
    c.last_name,
    c.email
HAVING COUNT(DISTINCT a.job_id) > 3
ORDER BY jobs_applied DESC, c.candidate_id;
```

Result summary:

```text
506 candidates applied to more than 3 jobs.
```

## Task Group 2: Data Modeling And Transformation

### Tool Choice

I chose dbt with DuckDB.

Reasoning:

- The assignment data is small enough to run locally.
- dbt clearly shows the transformation graph, tests, model contracts, and separation between staging and marts.
- DuckDB gives a lightweight analytical database without requiring Docker or a server.
- The same modeling approach can later move to a warehouse or lakehouse-backed dbt target.

### dbt Layers

#### Sources

`models/sources.yml` defines the raw DuckDB tables as dbt sources.

#### Staging Models

Staging models clean and normalize the raw data:

- `stg_jobs`
- `stg_candidates`
- `stg_educations`
- `stg_applications`
- `stg_workflow_events`

Main staging responsibilities:

- Trim string fields
- Normalize categorical fields to uppercase
- Lowercase emails
- Parse dirty date formats with `parse_date`
- Add surrogate event IDs for workflow events
- Deduplicate records using window functions

#### Intermediate Models

`int_workflow_events_enriched` joins workflow events to applications and adds:

- `apply_date`
- `is_anomaly`
- `event_sequence`

This layer keeps reusable business logic out of final marts.

#### Mart Models

The final analytical schema uses a star-schema style design.

Fact tables:

- `fct_applications`
- `fct_workflow_events`

Dimension tables:

- `dim_job`
- `dim_candidate`

### Star Schema

```text
dim_candidate
      |
      | candidate_id
      v
fct_applications ---- job_id ----> dim_job
      |
      | application_id
      v
fct_workflow_events
```

### Fact Tables

#### `fct_applications`

Grain: one row per application.

Important fields:

- `application_id`
- `job_id`
- `candidate_id`
- `apply_date`
- `hired_date`
- `current_status`
- `is_hired`
- `time_to_hire_days`

Business logic:

- `hired_date` is the first valid `HIRED` event after the application date.
- `current_status` is the latest workflow status by event timestamp.
- `time_to_hire_days` is calculated only for valid hired applications.
- Hired-before-applied anomalies are excluded from the hire-date calculation but are still preserved for auditing.

#### `fct_workflow_events`

Grain: one row per workflow event.

This table supports lifecycle, funnel, and status-transition analysis.

### Dimension Tables

#### `dim_job`

Contains the latest known job attributes:

- `job_id`
- `title`
- `department`
- `posted_date`
- `status`

#### `dim_candidate`

Contains candidate attributes enriched with education:

- `candidate_id`
- `first_name`
- `last_name`
- `email`
- `phone`
- `skills`
- `degree`
- `institution`
- `year`

### SCD Strategy

For this assignment, dimensions are modeled as SCD Type 1:

- Keep the latest known state.
- Use `_ingestion_ts` to choose the latest version.
- Keep the design simple and query-friendly.

In production, I would consider SCD Type 2 for attributes where history matters, such as job status, candidate contact details, or department changes. That would add:

- `effective_from`
- `effective_to`
- `is_current`

## Time To Hire Metric

The assignment asks for time to hire by job and department.

The core calculation is:

```sql
DATE_DIFF('day', apply_date, hired_date)
```

The model-level metric is available in `main_marts.fct_applications.time_to_hire_days`.

### Time To Hire By Department

```sql
SELECT
    j.department,
    COUNT(*) AS hired_applications,
    AVG(a.time_to_hire_days) AS avg_time_to_hire_days
FROM icims.main_marts.fct_applications a
JOIN icims.main_marts.dim_job j
    ON a.job_id = j.job_id
WHERE a.is_hired
GROUP BY j.department
ORDER BY hired_applications DESC;
```

Observed result from the current run:


| Department  | Hired applications | Avg time to hire days |
| ----------- | ------------------ | --------------------- |
| MARKETING   | 225                | 29.83                 |
| ENGINEERING | 193                | 30.94                 |
| FINANCE     | 192                | 29.82                 |
| PRODUCT     | 189                | 30.17                 |
| SALES       | 184                | 31.18                 |
| HR          | 129                | 30.84                 |


### Time To Hire By Job

```sql
SELECT
    job_id,
    COUNT(*) AS hired_applications,
    AVG(time_to_hire_days) AS avg_time_to_hire_days
FROM icims.main_marts.fct_applications
WHERE is_hired
GROUP BY job_id
ORDER BY hired_applications DESC, avg_time_to_hire_days
LIMIT 10;
```

## Task Group 3: Engineering, Quality And Optimization

### Idempotency

The pipeline is designed so the same source file and same `run_date` can be processed more than once without duplicating analytical results.

Implemented locally:

- Each source load writes to `raw.ingestion_batches`.
- File checksums are used to detect previously loaded files.
- Snapshot-style sources such as jobs, candidates, and applications skip a file if the same successful batch already exists.
- Batch-replay sources such as education and workflow events delete the same `_batch_id` before reloading, so reruns are deterministic.
- dbt staging models deduplicate by business keys such as `job_id`, `candidate_id`, `application_id`, and `event_id`.
- dbt mart models use incremental materialization with `unique_key` values.
- Workflow-event enrichment and Time to Hire aggregates process impacted applications/jobs instead of blindly appending duplicate rows.

This keeps raw and modeled row counts stable on rerun. In production I would use the same principles with object-store file checksums, Iceberg/Delta transaction metadata, `MERGE`, and batch audit states such as `STARTED`, `SUCCEEDED`, and `FAILED`.

### Data Quality

Implemented dbt checks include:

- `not_null`
- `unique`
- `accepted_values`
- email format validation
- invalid application date validation
- invalid posted date validation
- hired-before-applied anomaly detection
- source freshness checks
- raw source row-count volume checks

Source freshness is configured in `models/sources.yml` using `_ingestion_ts` as the loaded-at field:

```bash
dbt source freshness --project-dir dbt/icims_project --vars '{"run_date":"2026-05-07"}'
```

Volume anomaly checks use a reusable dbt generic test, `row_count_between`, to catch unexpected source row-count drops or spikes. For example, the local `workflow_events` source is expected to be between 15,000 and 18,000 rows for the provided assignment dataset.

The hired-before-applied anomaly is handled in two ways:

1. A dbt singular test detects it and reports it as a warning.
2. A persisted audit model, `main_quality.dq_hired_before_applied_anomalies`, stores the anomalous records for investigation.

The anomaly detection logic is:

```sql
SELECT *
FROM {{ ref('stg_workflow_events') }} w
JOIN {{ ref('stg_applications') }} a
    ON w.application_id = a.application_id
WHERE w.new_status = 'HIRED'
  AND w.event_timestamp < a.apply_date
```

Observed anomaly count:

```text
1 hired-before-applied record
```

Handling strategy:

- Do not delete the record from raw or staging.
- Flag it as an anomaly.
- Exclude it from `hired_date` and `time_to_hire_days` calculations.
- Store failures with `dbt test --store-failures` so data quality issues are auditable.
- Alert data owners in production and correct the upstream source if the event timestamp or application timestamp is wrong.

In production, I would keep dbt tests for deterministic warehouse checks and add a monitoring layer such as Anomalo, Monte Carlo, Soda, or Great Expectations for automated anomaly detection, trend-based volume monitoring, and alerting.

### Unit Tests

The project includes Python unit tests for ingestion helper functions in `tests/test_ingestion_common.py`.

They validate:

- file checksum generation
- deterministic batch ID generation
- batch ID changes when file content changes
- stable row hashing
- null normalization in hashes

Run them with:

```bash
python3 -m pytest tests
```

dbt tests are also part of the unit/data test strategy for SQL models:

```bash
dbt test --project-dir dbt/icims_project --vars '{"run_date":"2026-05-07"}'
```

### 10TB Workflow Events Scaling Design

If `workflow_events.jsonl` were 10TB, I would not load it with pandas or a single local DuckDB process. I would move the event pipeline to a lakehouse architecture.

Recommended AWS architecture:

```text
Source event stream or bulk files
  -> S3 landing zone
  -> AWS Glue or EMR Spark ingestion
  -> Bronze Apache Iceberg tables on S3
  -> Silver cleaned/deduplicated Iceberg tables
  -> dbt transformations for gold facts, dimensions, and aggregates
  -> Athena, Trino, or Spark SQL query layer
  -> CloudWatch metrics and alerts
```

Storage choice:

- Use Apache Iceberg backed by Parquet on S3.
- Iceberg gives ACID writes, schema evolution, hidden partitioning, time travel, and efficient `MERGE`.
- Parquet avoids repeatedly scanning and parsing JSONL.
- AWS Glue Catalog stores table metadata.
- Lake Formation controls table, column, and row-level access.

Partitioning and layout:

- Partition workflow events by event date or ingestion date.
- Sort or cluster by `application_id` and `event_timestamp` because downstream recomputation is application-centric.
- Compact small files regularly.
- Track min/max statistics for pruning.

Ingestion strategy:

- Read source files in parallel using Spark on Glue or EMR.
- Validate schema at ingestion.
- Add ingestion metadata and file lineage.
- Write malformed records to a quarantine table.
- Make batch writes idempotent through batch audit metadata and overwrite-by-partition or MERGE logic.

Transformation strategy:

- Process only new or changed partitions.
- Recompute impacted `application_id` values rather than rebuilding the full fact table.
- Use incremental MERGE into silver and gold tables.
- Keep raw bronze immutable for replay and audit.

Performance techniques:

- Avoid full scans by partition pruning.
- Avoid repeated JSON parsing by converting once to Parquet.
- Broadcast small dimensions where appropriate.
- Use adaptive query execution in Spark.
- Maintain table statistics.
- Use compaction to avoid small-file overhead.
- Use application-level change sets for late-arriving events.

Operational controls:

- Data quality checks for uniqueness, nulls, accepted statuses, date validity, and volume anomalies.
- Freshness checks on event arrival.
- Metrics for rows read, rows written, rejected rows, duplicate rate, and late-arriving event rate.
- Alerting when volume or anomaly counts deviate from expected ranges.

## Known Trade-Offs

- DuckDB is for the local assignment only, but a distributed lakehouse is better for multi-TB event streams.
- Raw ingestion currently favors simplicity and traceability. A production loader should add deterministic batch IDs and true batch-level upsert/retry behavior.
- Dimensions are SCD Type 1 for simplicity. SCD Type 2 would be better when historical attribute changes are analytically important.
- The current project uses dbt tests for data quality. For production, I would add orchestration-level checks and pipeline observability (Or we can have framework like Anamalo for Automated and low-maintenance monitoring)

## AI Usage Statement

I used AI assistance to speed up boilerplate, structure documentation. I reviewed the generated suggestions against the assignment requirements and the actual project code before including them.

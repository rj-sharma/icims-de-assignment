# iCIMS Data Engineering Take-Home Assignment

## Overview

This repository contains my solution for the iCIMS Data Engineering assignment.

- Ingestion pipeline (Task Group 1) loads source files into DuckDB raw tables.
- dbt models transform raw data into staging and marts layers.
- Basic data quality checks are implemented with dbt tests.

## Assignment Coverage

- Task Group 1: Implemented
  - Database setup and ingestion into `raw` schema
  - Idempotent loading with `CREATE OR REPLACE TABLE`
- Task Group 2: In progress / partial
  - Staging and mart models in dbt
  - `time_to_hire`, `is_hired`, and `current_status` metrics in marts

## Project Structure

```text
.
├── data/                           # Input files provided in assignment
├── src/ingestion/load_data.py      # Task 1 ingestion script
├── dbt/icims_project/              # dbt project (staging + marts)
├── requirements.txt
└── ReadMe.md
```

## Tech Stack

- Python 3.9+ (tested with local Python)
- DuckDB
- pandas
- dbt-core
- dbt-duckdb

## Prerequisites

- `python3` available on PATH
- `pip` available
- macOS/Linux shell commands below (Windows users can run equivalent commands in PowerShell)

## Quick Start (From Scratch)

Run all commands from the repository root:

```bash
cd /path/to/icims-de-assignment
```

### 1) Create and activate virtual environment

```bash
python3 -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows
```

### 2) Install dependencies

```bash
pip install -r requirements.txt
```

### 3) Configure dbt profile

Create or update `~/.dbt/profiles.yml` with:

```yaml
icims_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/icims-de-assignment/icims.duckdb
      threads: 4
```

Important: use an absolute path (recommended) or ensure you always run dbt from the same working directory. A wrong path is the most common setup issue.

### 4) Load raw data (Task Group 1)

```bash
python src/ingestion/load_data.py
```

This creates/refreshes:

- `raw.jobs`
- `raw.candidates`
- `raw.education`
- `raw.applications`
- `raw.workflow_events`

### 5) Run dbt transformations

```bash
dbt run --project-dir dbt/icims_project
dbt test --project-dir dbt/icims_project
```

### 6) Optional: run a single model

```bash
dbt run --project-dir dbt/icims_project --select dim_candidates
```

Or with dependencies:

```bash
dbt run --project-dir dbt/icims_project --select +dim_candidates
```

## Validation Queries

Use DuckDB to quickly verify outputs:

```bash
python3 - <<'PY'
import duckdb
con = duckdb.connect("icims.duckdb")
print("Raw tables:", con.execute("SHOW TABLES FROM raw").fetchall())
print("fct_applications rows:", con.execute("SELECT COUNT(*) FROM fct_applications").fetchone()[0])
print("Open jobs:", con.execute("SELECT COUNT(*) FROM raw.jobs WHERE lower(status)='open'").fetchone()[0])
PY
```

## Re-run Setup from a Clean State

To clean generated artifacts and re-test setup docs:

```bash
bash scripts/reset_env.sh
```

What it removes:

- Local DuckDB artifacts: `icims.duckdb`, `dbt/icims_project/icims.duckdb`
- dbt build artifacts: `dbt/icims_project/target`, `dbt/icims_project/dbt_packages`
- log folders: `logs`, `dbt/logs`

To also remove the virtual environment:

```bash
bash scripts/reset_env.sh --remove-venv
```

## Common Errors and Fixes

1. `Table ... does not exist`
   - Ensure `python src/ingestion/load_data.py` was run successfully.
   - Ensure `profiles.yml` points to the correct `icims.duckdb`.

2. dbt model fails due to missing columns
   - Run dependencies too: `dbt run --project-dir dbt/icims_project --select +<model_name>`.

3. dbt cannot connect to DuckDB
   - Verify `path` in `~/.dbt/profiles.yml` is valid and accessible.
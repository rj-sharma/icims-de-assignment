2# iCIMS Data Engineering Take-Home Assignment

## 📌 Overview

This project implements an end-to-end data pipeline using a **Medallion Architecture (Bronze → Silver → Gold)** to process recruitment data and generate analytical datasets.

The solution focuses on:
- Data ingestion from multiple formats (CSV, JSON, JSONL)
- Transformation using dbt
- Dimensional modeling (Star Schema)
- Data quality and idempotency

---

## 🏗️ Architecture

### 🥉 Bronze Layer (Raw)
- Data ingested into DuckDB using Python
- Raw tables created with minimal transformation
- Idempotent ingestion using `CREATE OR REPLACE`

### 🥈 Silver Layer (Staging - dbt)
- Data cleaning and standardization
- Handles:
  - Type casting
  - Deduplication
  - String normalization
- Models: `stg_*`

### 🥇 Gold Layer (Marts)
- Star schema design for analytics
- Fact and dimension tables:
  - `fct_applications`
  - `fct_workflow_events`
  - `dim_job`
  - `dim_candidate`

---

## 📊 Data Model

Based on the provided ERD:

- **Fact Tables**
  - `fct_applications`: Application lifecycle with derived metrics
  - `fct_workflow_events`: Status change history

- **Dimension Tables**
  - `dim_job`: Job attributes
  - `dim_candidate`: Candidate profile with nested education

- **Key Metrics**
  - `time_to_hire`
  - `is_hired`
  - `current_status`

---

## ⚙️ Tech Stack

- **DuckDB** → Analytical database
- **dbt (DuckDB adapter)** → Transformations
- **Python (pandas)** → Ingestion

---

## 🚀 Setup Instructions

### 1. Clone Repository

```bash
git clone <your-repo-url>
cd icims-de-assignment
```

### 2. Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

#### 4. Configure dbt Profile
edit - `~/.dbt/profiles.yml`

add below in the file
```
icims_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../../icims.duckdb
      threads: 4
```

### 5. Load Raw Data (Bronze Layer)

```bash 
python src/ingestion/load_data.py
```


### 6. Run dbt Models (Silver + Gold)

```bash
cd dbt/icims_project
dbt run
dbt test
```



## Validation

Run sample query:
```python
import duckdb

con = duckdb.connect("icims.duckdb")
con.execute("SELECT * FROM fct_applications LIMIT 10").fetchall()
```

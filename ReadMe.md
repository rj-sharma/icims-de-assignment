# 🚀 Quick Start

## 1. Environment Setup

```bash
pip install -r requirements.txt
```

## 2. Ingest Raw Data

```bash
python src/ingestion/load_data.py
```

## 3. Run Transformations & Tests

```bash
cd dbt/icims_project
dbt run
dbt test
```


--- 
# 🏗️ Architecture

## Local (Assignment)
Python Ingestion → DuckDB (raw) → dbt (staging → marts)

## Scalable (Lakehouse)
S3 (Parquet/Iceberg)
↓
Spark (Glue/EMR)
↓
Curated (Silver)
↓
Gold (Facts/Dimensions)
↓
Warehouse / BI

---

---

# 🛠 Engineering Workflow

## I. Ingestion (Task Group 1)

- Data is ingested into **raw schema** using Python
- Append-only pattern with `_ingestion_ts`, `_batch_id`
- Supports re-runs and traceability

---

## II. dbt Modeling Architecture

### **Staging (`stg_`)**
- Data cleaning & normalization
- Date standardization via reusable macro
- Deduplication within batch
- Incremental using `run_date`

---

# ⭐ Task 2: Star Schema Design & Time-to-Hire

## 🔷 Overview

A **star schema** is designed to enable efficient analytical queries and clear separation of concerns between dimensions and facts.

---

## 🧩 Fact Tables

### **1. `fct_applications` (Core Fact)**

Grain: **1 row per application**

**Purpose:**
- Central analytical table
- Computes business metrics

**Key Fields:**
- `application_id` (PK)
- `job_id`, `candidate_id` (FKs)
- `apply_date`
- `hired_date`
- `current_status`
- `is_hired`
- `time_to_hire_days`

**Key Logic:**
- `hired_date` derived from workflow events
- `current_status` = latest event
- `time_to_hire_days` = difference between apply and hire date

**Incremental Strategy:**
- Recomputes only impacted `application_id`
- Handles late-arriving events

---

### **2. `fct_workflow_events`**

Grain: **1 row per event**

**Purpose:**
- Tracks lifecycle changes
- Enables funnel and status analysis

---

## 🧩 Dimension Tables

### **1. `dim_job`**

**Fields:**
- `job_id`
- `title`, `department`
- `posted_date`
- `status`

**Design:**
- Incremental (latest state)

---

### **2. `dim_candidate`**

**Fields:**
- `candidate_id`
- `name, email, phone`
- `skills`
- `education details`

**Design:**
- Enriched from multiple sources
- Flattened for analytics

---

## 🔄 SCD Strategy

### ✅ Implemented (Demo)

- **SCD Type 1 (overwrite latest state)**
- Uses `_ingestion_ts` for latest record selection
- Keeps design simple and performant

---

### 🏗 Ideal Production Design (SCD Type 2)

In a real system, dimensions would support full history:

**Additional Columns:**
- `effective_from`
- `effective_to`
- `is_current`

**Behavior:**
- New record inserted when attributes change
- Old record expired (`effective_to` updated)

**Example:**
- Job status changes → new row created
- Candidate updates email → tracked historically

---

## ⚠️ Anomaly Handling

### Business Rule:
A candidate **cannot be hired before applying**

### Implementation:
- Inline anomaly flag in fact logic:
```sql
CASE 
  WHEN new_status = 'HIRED' 
   AND event_timestamp < apply_date 
  THEN TRUE ELSE FALSE 
END AS is_anomaly
```

### Approach:
- Data is **not filtered**
- Anomalies are:
  - surfaced via dbt tests
  - queryable via audit tables

---

## 📈 Data Quality

### Generic Tests
- `not_null`
- `unique`
- `accepted_values`

### Custom Test
- `hired_before_applied`

```bash
dbt test --store-failures
```

Query failures:
```sql
SELECT * FROM icims.main_dbt_test__audit.hired_before_applied;
```

---

## 🧠 Key Design Decisions

- Separate staging vs business logic
- Avoid filtering anomalies at transformation layer
- Use key-based incremental strategy (not date filtering)
- Preserve raw data fidelity

---

## 🚀 Key Business Metric

### Time to Hire

```sql
DATE_DIFF('day', apply_date, hired_date)
```

- Uses **cleaned + validated data**
- Excludes anomalies
- Supports accurate reporting

---

# 🔷 Task 3: Engineering & Optimization

Idempotency:
- Raw reset for demo
- dbt incremental models
- run_date parameter

Data Quality:
- dbt tests (unique, not_null)
- anomaly flag: hired_date < applied_date

Scaling (10TB):
- Lakehouse: S3 + Parquet + Iceberg
- Spark (EMR/Glue)

Micro-batch Dedup:
- Dedup within batch
- Merge using keys

Partitioning:
- event_date

Partition Overwrite:
.mode("overwrite")
.option("replaceWhere", "event_date = '2026-05-05'")

Benefits:
- Efficient backfills
- Scalable
- Idempotent

Unit Testing:
- Python + dbt tests

---

# 🛠️ Orchestration

create_tables → ingestion → stg → dim → fct → test

Supports incremental + full runs

---

# 🚀 Final Thoughts

Demonstrates:
- End-to-end pipeline
- Data modeling
- Scalable architecture
- Production practices

--

# 🛠 Troubleshooting

| Issue | Fix |
|------|-----|
| Table not found | Run ingestion first |
| dbt error | Check DuckDB path |
| Missing data | Ensure correct `run_date` |

---

# ⭐ Config

```yaml
icims_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/icims.duckdb
      threads: 4
```

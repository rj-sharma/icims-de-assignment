# 🚀 Quick Start

## 1. Environment Setup
```bash
# Install required packages
pip install -r requirements.txt
```

## 2. Ingest Raw Data
Run the ingestion script to initialize the `icims.duckdb` database and load the raw schema.

```bash
python src/ingestion/load_data.py
```

## 3. Run Transformations & Tests
Navigate to the dbt project directory to build the models.

```bash
cd dbt/icims_project

# Run models
dbt run

# Run data quality tests
dbt test
```

---

# 🛠 Engineering Workflow

## I. Ingestion (Task Group 1)
The `load_data.py` script serves as the primary entry point.  
It ensures **idempotency** by utilizing `CREATE OR REPLACE TABLE` logic.

**Schema:** All data is initially loaded into the `raw` schema.  
**Tables:** `jobs`, `candidates`, `education`, `applications`, `workflow_events`.

---

## II. dbt Modeling Architecture
We follow a standard **medallion-style architecture** to ensure data lineage and quality:

### **Staging (`stg_`)**
- Normalizes column names  
- Applies type casting  
- Standardizes date formats  

### **Marts (`dim_`, `fct_`)**
Final analytical models, including:

- `fct_applications`
- `dim_candidates`

These include important business metrics such as:

- `time_to_hire`
- `is_hired`

---

## III. Data Quality
We utilize **dbt’s testing framework** to maintain high integrity:

### **Generic Tests**
- `unique`
- `not_null`
- `accepted_values`

### **Custom Tests**
Located in `tests/`, including:

- `valid_dates.sql` → Ensures application dates occur before hiring dates.

---

# 📊 Analytical Insights (Task 1)

### **a) How many jobs are currently open?**
```sql
SELECT COUNT(*) 
FROM raw.jobs 
WHERE lower(status) = 'open';
```

### **b) Top 5 departments by number of applications**
```sql
SELECT j.department,
       COUNT(a.application_id) AS total_applications
FROM raw.applications a
JOIN raw.jobs j ON a.job_id = j.job_id
GROUP BY j.department
ORDER BY total_applications DESC
LIMIT 5;
```

### **c) Candidates who applied to more than 3 jobs**
```sql
SELECT candidate_id,
       COUNT(DISTINCT job_id) AS jobs_applied
FROM raw.applications
GROUP BY candidate_id
HAVING COUNT(DISTINCT job_id) > 3;
```

---

# 🛠 Troubleshooting & Notes

| Issue | Fix / Explanation |
|-------|-------------------|
| **Table Not Found** | Ensure you run ingestion first: `python load_data.py` |
| **dbt Connection Error** | Verify the DuckDB path in `profiles.yml` is absolute |
| **Model Failure** | Ensure all staging models are built before marts |

---

### ⭐ Important Configuration Note

To run dbt, ensure your `profiles.yml` is configured as follows:

```yaml
icims_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/icims.duckdb  # Update this!
      threads: 4
```

# iCIMS Data Engineering Take-Home Assignment

## Overview

This repository contains my solution for the iCIMS Data Engineering assignment. It includes:

- A Python ingestion pipeline using DuckDB (Task Group 1)
- dbt transformations (staging + marts)
- Data validation tests
- SQL answers for the analytical questions from Task Group 1

---

## Project Structure


.
├── data/ # Raw CSV files provided in the assignment
├── src/
│ └── ingestion/
│ └── load_data.py # Ingestion script for Task Group 1
├── dbt/
│ └── icims_project/ # dbt project (sources, staging, marts, tests)
├── sql/
│ └── task1_analysis.sql # SQL answers for Task 1 analytical questions
├── requirements.txt
└── README.md


---

## Tech Stack

- Python 3.9+
- DuckDB
- pandas
- dbt-core
- dbt-duckdb

---

# 1. Ingestion Pipeline (Task Group 1)

The ingestion process loads raw CSV files into DuckDB under the `raw` schema.

### Raw tables created

- `raw.jobs`
- `raw.candidates`
- `raw.education`
- `raw.applications`
- `raw.workflow_events`

### Ingestion properties

- Idempotent loads using `CREATE OR REPLACE TABLE`
- Uses pandas + DuckDB for fast ingestion
- All files sourced from the `data/` directory

### Run ingestion

```bash
python src/ingestion/load_data.py
2. dbt Project Structure

dbt project located at:

dbt/icims_project/
Staging models
Standardized column naming
Date parsing for multiple formats
Type normalization
Source freshness tests
Mart models
dim_candidates
fct_applications
Derived business logic fields:
is_hired
current_status
time_to_hire
Data quality tests
Unique tests
Not-null tests
Accepted values tests
Custom SQL tests
3. Running dbt
Install dependencies
pip install -r requirements.txt
Configure dbt profile

Add this to ~/.dbt/profiles.yml:

icims_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/icims.duckdb
      threads: 4
Run dbt models
dbt run --project-dir dbt/icims_project
Run dbt tests
dbt test --project-dir dbt/icims_project
Run a single model
dbt run --project-dir dbt/icims_project --select dim_candidates
4. SQL Answers for Task Group 1
a) How many jobs are currently open?
SELECT COUNT(*)
FROM raw.jobs
WHERE lower(status) = 'open';
b) Top 5 departments by number of applications
SELECT j.department,
       COUNT(a.application_id) AS total_applications
FROM raw.applications a
JOIN raw.jobs j 
  ON a.job_id = j.job_id
GROUP BY j.department
ORDER BY total_applications DESC
LIMIT 5;
c) Candidates who applied to more than 3 jobs
SELECT candidate_id,
       COUNT(DISTINCT job_id) AS jobs_applied
FROM raw.applications
GROUP BY candidate_id
HAVING COUNT(DISTINCT job_id) > 3;
5. Validation Queries
python3 - <<'PY'
import duckdb
con = duckdb.connect("icims.duckdb")
print("Raw tables:", con.execute("SHOW TABLES FROM raw").fetchall())
print("Applications fact count:",
      con.execute("SELECT COUNT(*) FROM fct_applications").fetchone()[0])
PY
6. Troubleshooting
Issue	Resolution
Missing tables	Run ingestion: python load_data.py
dbt connection issues	Ensure path in profiles.yml is correct
Tests failing	Confirm staging models built before marts
Schema mismatch	Clear artifacts and rebuild
7. Notes
Ingestion is fully idempotent.
dbt may create schemas like main_staging depending on defaults.
Date parsing is handled in dbt; can be shifted to ingestion later.
8. Possible Enhancements

If extended time were available:

Incremental dbt models
Snapshotting for slowly changing jobs/candidates
More advanced anomaly detection tests
Auto-generated dbt docs & lineage visualization
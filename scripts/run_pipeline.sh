#!/bin/bash

set -e  # fail fast

# -------------------------------
# DEFAULTS
# -------------------------------
RUN_DATE=$(date +%F)
MODE="incremental"   # default

# -------------------------------
# ARGUMENT PARSING
# -------------------------------
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --date) RUN_DATE="$2"; shift ;;
        --full) MODE="full" ;;
        *) echo "❌ Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

echo "========================================"
echo "🚀 Starting Pipeline"
echo "Run Date: $RUN_DATE"
echo "Mode: $MODE"
echo "========================================"

# -------------------------------
# STEP 1: INGESTION
# -------------------------------
echo "📥 Running ingestion..."
python3 src/ingestion/load_data.py

# -------------------------------
# STEP 2: DBT VARS
# -------------------------------
DBT_VARS="{run_date: '$RUN_DATE'}"

echo "🧪 DBT_VARS = $DBT_VARS"

if [ "$MODE" == "full" ]; then
    FULL_REFRESH="--full-refresh"
else
    FULL_REFRESH=""
fi

# -------------------------------
# STEP 3: STAGING
# -------------------------------
echo "🔄 Running staging models..."
dbt run \
  --project-dir dbt/icims_project \
  --vars "$DBT_VARS" \
  $FULL_REFRESH \
  --select tag:stg

# -------------------------------
# STEP 4: INTERMEDIATE
# -------------------------------
echo "🔄 Running intermediate models..."
dbt run \
  --project-dir dbt/icims_project \
  --vars "$DBT_VARS" \
  $FULL_REFRESH \
  --select tag:int

# -------------------------------
# STEP 5: DIMENSIONS
# -------------------------------
echo "📊 Running dimension models..."
dbt run \
  --project-dir dbt/icims_project \
  --vars "$DBT_VARS" \
  $FULL_REFRESH \
  --select tag:dim

# -------------------------------
# STEP 6: FACTS
# -------------------------------
echo "📊 Running fact models..."
dbt run \
  --project-dir dbt/icims_project \
  --vars "$DBT_VARS" \
  $FULL_REFRESH \
  --select tag:fact

# -------------------------------
# STEP 7: TESTS
# -------------------------------
echo "✅ Running data quality tests..."
dbt test \
  --project-dir dbt/icims_project \
  --vars "$DBT_VARS"
import duckdb
import pandas as pd
import os
import uuid
import json
from datetime import datetime

DB_PATH = "icims.duckdb"
DATA_DIR = "data"


def get_connection():
    print("DB PATH:", os.path.abspath(DB_PATH))
    return duckdb.connect(DB_PATH)


def ingest_applications(con):
    file_path = os.path.join(DATA_DIR, "applications.csv")
    print(f"Ingesting: {file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        print("WARNING: applications.csv is empty")
        return  
    # Metadata columns
    batch_id = str(uuid.uuid4())
    ingestion_ts = datetime.utcnow()

    df["_ingestion_ts"] = ingestion_ts
    df["_batch_id"] = batch_id
    df["_file_name"] = "applications.csv"

    # Register dataframe
    con.register("apps_df", df)

    # Append-only insert
    con.execute("""
        INSERT INTO raw.applications
        SELECT * FROM apps_df
    """)

    print(f"Loaded {len(df)} rows into raw.applications | batch_id={batch_id}")


def ingest_education(con):
    file_path = os.path.join(DATA_DIR, "education.csv")
    print(f"Ingesting: {file_path}")

    df = pd.read_csv(file_path)

    batch_id = str(uuid.uuid4())
    ingestion_ts = datetime.utcnow()

    df["_ingestion_ts"] = ingestion_ts
    df["_batch_id"] = batch_id
    df["_file_name"] = "education.csv"

    con.register("edu_df", df)

    con.execute("""
        INSERT INTO raw.education
        SELECT * FROM edu_df
    """)

    print(f"Loaded {len(df)} rows into raw.education | batch_id={batch_id}")

def ingest_candidates(con):
    file_path = os.path.join(DATA_DIR, "candidates.json")
    print(f"Ingesting: {file_path}")

    with open(file_path) as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    batch_id = str(uuid.uuid4())
    ingestion_ts = datetime.utcnow()

    df["_ingestion_ts"] = ingestion_ts
    df["_batch_id"] = batch_id
    df["_file_name"] = "candidates.json"

    # Convert skills to string (important for DuckDB simplicity)
    if "skills" in df.columns:
        df["skills"] = df["skills"].apply(lambda x: ",".join(x) if isinstance(x, list) else str(x))

    con.register("candidates_df", df)

    con.execute("""
        INSERT INTO raw.candidates
        SELECT * FROM candidates_df
    """)

    print(f"Loaded {len(df)} rows into raw.candidates | batch_id={batch_id}")

def ingest_jobs(con):
    file_path = os.path.join(DATA_DIR, "jobs.csv")
    print(f"Ingesting: {file_path}")

    df = pd.read_csv(file_path)

    batch_id = str(uuid.uuid4())
    ingestion_ts = datetime.utcnow()

    df["_ingestion_ts"] = ingestion_ts
    df["_batch_id"] = batch_id
    df["_file_name"] = "jobs.csv"

    con.register("jobs_df", df)

    con.execute("""
        INSERT INTO raw.jobs
        SELECT * FROM jobs_df
    """)

    print(f"Loaded {len(df)} rows into raw.jobs | batch_id={batch_id}")

def ingest_workflow_events(con):
    file_path = os.path.join(DATA_DIR, "workflow_events.jsonl")
    print(f"Ingesting: {file_path}")

    df = pd.read_json(file_path, lines=True)

    batch_id = str(uuid.uuid4())
    ingestion_ts = datetime.utcnow()

    df["_ingestion_ts"] = ingestion_ts
    df["_batch_id"] = batch_id
    df["_file_name"] = "workflow_events.jsonl"

    con.register("events_df", df)

    con.execute("""
        INSERT INTO raw.workflow_events
        SELECT * FROM events_df
    """)

    print(f"Loaded {len(df)} rows into raw.workflow_events | batch_id={batch_id}")

def load_data():
    con = get_connection()

    ingest_applications(con)

    ingest_education(con)

    ingest_candidates(con)

    ingest_jobs(con)

    ingest_workflow_events(con)

    con.close()
    print("Ingestion complete.")


if __name__ == "__main__":
    load_data()
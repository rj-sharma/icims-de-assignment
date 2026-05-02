import duckdb
import pandas as pd
import json
import os

DB_PATH = "icims.duckdb"

def load_data():
    print("DB PATH:", os.path.abspath(DB_PATH))
    con = duckdb.connect(DB_PATH)

    # Create schema
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Jobs
    jobs = pd.read_csv("data/jobs.csv")
    con.register("jobs_df", jobs)
    con.execute("CREATE OR REPLACE TABLE raw.jobs AS SELECT * FROM jobs_df")

    # Candidates
    with open("data/candidates.json") as f:
        candidates = pd.DataFrame(json.load(f))
    con.register("candidates_df", candidates)
    con.execute("CREATE OR REPLACE TABLE raw.candidates AS SELECT * FROM candidates_df")

    # Education
    edu = pd.read_csv("data/education.csv")
    con.register("edu_df", edu)
    con.execute("CREATE OR REPLACE TABLE raw.education AS SELECT * FROM edu_df")

    # Applications
    apps = pd.read_csv("data/applications.csv")
    con.register("apps_df", apps)
    con.execute("CREATE OR REPLACE TABLE raw.applications AS SELECT * FROM apps_df")

    # Workflow Events
    workflow_events = pd.read_json("data/workflow_events.jsonl", lines=True)
    con.register("events_df", workflow_events)
    con.execute("CREATE OR REPLACE TABLE raw.workflow_events AS SELECT * FROM events_df")

    # Debug
    print("Schemas:", con.execute("SHOW SCHEMAS").fetchall())
    print("Raw Tables:", con.execute("SHOW TABLES FROM raw").fetchall())

    print("Data loaded successfully!")
    con.commit()

if __name__ == "__main__":
    load_data()
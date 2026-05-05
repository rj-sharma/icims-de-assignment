import duckdb
import os

DB_PATH = "icims.duckdb"


def get_connection():
    print("DB PATH:", os.path.abspath(DB_PATH))
    return duckdb.connect(DB_PATH)


def create_schema_and_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    print("⚠️ Dropping existing tables (reset mode)...")

    con.execute("DROP TABLE IF EXISTS raw.applications")
    con.execute("DROP TABLE IF EXISTS raw.education")
    con.execute("DROP TABLE IF EXISTS raw.candidates")
    con.execute("DROP TABLE IF EXISTS raw.jobs")
    con.execute("DROP TABLE IF EXISTS raw.workflow_events")

    # Applications
    con.execute("""
        CREATE TABLE raw.applications (
            application_id VARCHAR,
            job_id VARCHAR,
            candidate_id VARCHAR,
            apply_date VARCHAR,
            _ingestion_ts TIMESTAMP,
            _batch_id VARCHAR,
            _file_name VARCHAR
        )
    """)

    # Education
    con.execute("""
        CREATE TABLE raw.education (
            candidate_id VARCHAR,
            degree VARCHAR,
            institution VARCHAR,
            year INTEGER,
            _ingestion_ts TIMESTAMP,
            _batch_id VARCHAR,
            _file_name VARCHAR
        )
    """)

    # Candidates
    con.execute("""
        CREATE TABLE raw.candidates (
            candidate_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            skills VARCHAR,
            _ingestion_ts TIMESTAMP,
            _batch_id VARCHAR,
            _file_name VARCHAR
        )
    """)

    # Jobs
    con.execute("""
        CREATE TABLE raw.jobs (
            job_id VARCHAR,
            title VARCHAR,
            department VARCHAR,
            posted_date VARCHAR,
            status VARCHAR,
            _ingestion_ts TIMESTAMP,
            _batch_id VARCHAR,
            _file_name VARCHAR
        )
    """)

    # Workflow Events
    con.execute("""
        CREATE TABLE raw.workflow_events (
            application_id VARCHAR,
            old_status VARCHAR,
            new_status VARCHAR,
            event_timestamp VARCHAR,
            _ingestion_ts TIMESTAMP,
            _batch_id VARCHAR,
            _file_name VARCHAR
        )
    """)

    print("✅ Tables recreated successfully")


def main():
    con = get_connection()
    create_schema_and_tables(con)
    con.close()


if __name__ == "__main__":
    main()
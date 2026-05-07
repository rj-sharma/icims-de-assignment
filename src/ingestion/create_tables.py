import duckdb
import os

DB_PATH = os.getenv("ICIMS_DB_PATH", "icims.duckdb")


def get_connection():
    print("DB PATH:", os.path.abspath(DB_PATH))
    return duckdb.connect(DB_PATH)


def create_schema_and_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    print("Dropping existing tables (reset mode)...")

    con.execute("DROP TABLE IF EXISTS raw.applications")
    con.execute("DROP TABLE IF EXISTS raw.education")
    con.execute("DROP TABLE IF EXISTS raw.candidates")
    con.execute("DROP TABLE IF EXISTS raw.jobs")
    con.execute("DROP TABLE IF EXISTS raw.workflow_events")
    con.execute("DROP TABLE IF EXISTS raw.ingestion_batches")

    # Applications
    con.execute("""
        CREATE TABLE raw.applications (
            application_id VARCHAR,
            job_id VARCHAR,
            candidate_id VARCHAR,
            apply_date VARCHAR,
            _ingestion_ts TIMESTAMP,
            _ingestion_date DATE,
            _batch_id VARCHAR,
            _source_system VARCHAR,
            _file_name VARCHAR,
            _source_file_checksum VARCHAR,
            _record_hash VARCHAR
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
            _ingestion_date DATE,
            _batch_id VARCHAR,
            _source_system VARCHAR,
            _file_name VARCHAR,
            _source_file_checksum VARCHAR,
            _record_hash VARCHAR
        )
    """)

    # Candidates
    con.execute("""
        CREATE TABLE raw.candidates (
            candidate_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            email_hash VARCHAR,
            phone VARCHAR,
            phone_hash VARCHAR,
            skills VARCHAR,
            _ingestion_ts TIMESTAMP,
            _ingestion_date DATE,
            _batch_id VARCHAR,
            _source_system VARCHAR,
            _file_name VARCHAR,
            _source_file_checksum VARCHAR,
            _record_hash VARCHAR
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
            _ingestion_date DATE,
            _batch_id VARCHAR,
            _source_system VARCHAR,
            _file_name VARCHAR,
            _source_file_checksum VARCHAR,
            _record_hash VARCHAR
        )
    """)

    # Workflow Events
    con.execute("""
        CREATE TABLE raw.workflow_events (
            _event_id VARCHAR,
            application_id VARCHAR,
            old_status VARCHAR,
            new_status VARCHAR,
            event_timestamp VARCHAR,
            _event_date DATE,
            _ingestion_ts TIMESTAMP,
            _ingestion_date DATE,
            _batch_id VARCHAR,
            _source_system VARCHAR,
            _file_name VARCHAR,
            _source_file_checksum VARCHAR,
            _record_hash VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE raw.ingestion_batches (
            batch_id VARCHAR,
            source_name VARCHAR,
            source_system VARCHAR,
            file_name VARCHAR,
            file_checksum VARCHAR,
            load_strategy VARCHAR,
            status VARCHAR,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            rows_loaded INTEGER,
            error_message VARCHAR
        )
    """)

    print("Tables recreated successfully")


def main():
    con = get_connection()
    create_schema_and_tables(con)
    con.close()


if __name__ == "__main__":
    main()

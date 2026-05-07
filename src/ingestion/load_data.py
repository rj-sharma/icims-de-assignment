from common import get_connection
from ingest_applications import ingest_applications
from ingest_candidates import ingest_candidates
from ingest_education import ingest_education
from ingest_jobs import ingest_jobs
from ingest_workflow_events import ingest_workflow_events


def load_data():
    con = get_connection()

    try:
        ingest_applications(con)
        ingest_education(con)
        ingest_candidates(con)
        ingest_jobs(con)
        ingest_workflow_events(con)
    finally:
        con.close()

    print("Ingestion complete.")


if __name__ == "__main__":
    load_data()

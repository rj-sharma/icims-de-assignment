from datetime import datetime

import pandas as pd

from common import (
    data_file_path,
    deterministic_batch_id,
    file_checksum,
    mark_batch_failed,
    mark_batch_succeeded,
    stable_hash,
    start_batch,
)


def ingest_jobs(con):
    file_name = "jobs.csv"
    file_path = data_file_path(file_name)
    print(f"Ingesting: {file_path}")

    df = pd.read_csv(file_path)

    ingestion_ts = datetime.utcnow()
    source_name = "jobs"
    source_system = "job_requisition_snapshot"
    checksum = file_checksum(file_path)
    batch_id = deterministic_batch_id(source_name, file_name, checksum)

    df["_ingestion_ts"] = ingestion_ts
    df["_ingestion_date"] = ingestion_ts.date()
    df["_batch_id"] = batch_id
    df["_source_system"] = source_system
    df["_file_name"] = file_name
    df["_source_file_checksum"] = checksum
    df["_record_hash"] = df.apply(
        lambda row: stable_hash(
            [
                row["job_id"],
                row["title"],
                row["department"],
                row["posted_date"],
                row["status"],
            ]
        ),
        axis=1,
    )

    df = df[
        [
            "job_id",
            "title",
            "department",
            "posted_date",
            "status",
            "_ingestion_ts",
            "_ingestion_date",
            "_batch_id",
            "_source_system",
            "_file_name",
            "_source_file_checksum",
            "_record_hash",
        ]
    ]

    try:
        already_loaded = con.execute(
            """
            SELECT COUNT(*)
            FROM raw.ingestion_batches
            WHERE batch_id = ?
              AND status = 'SUCCEEDED'
            """,
            [batch_id],
        ).fetchone()[0]

        if already_loaded:
            print(f"Skipping raw.jobs; batch already loaded | batch_id={batch_id}")
            return

        con.register("jobs_df", df)
        start_batch(
            con,
            batch_id,
            source_name,
            source_system,
            file_name,
            checksum,
            "append_snapshot",
            ingestion_ts,
        )

        con.execute("""
            INSERT INTO raw.jobs
            SELECT * FROM jobs_df
        """)

        mark_batch_succeeded(con, batch_id, datetime.utcnow(), len(df))
    except Exception as exc:
        mark_batch_failed(con, batch_id, datetime.utcnow(), str(exc))
        raise

    print(f"Loaded {len(df)} rows into raw.jobs | batch_id={batch_id}")

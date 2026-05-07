import pandas as pd
from datetime import datetime

from common import (
    data_file_path,
    deterministic_batch_id,
    file_checksum,
    mark_batch_failed,
    mark_batch_succeeded,
    stable_hash,
    start_batch,
)


def ingest_applications(con):
    file_name = "applications.csv"
    file_path = data_file_path(file_name)
    print(f"Ingesting: {file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        print("WARNING: applications.csv is empty")
        return

    ingestion_ts = datetime.utcnow()
    source_name = "applications"
    source_system = "application_daily_extract"
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
                row["application_id"],
                row["job_id"],
                row["candidate_id"],
                row["apply_date"],
            ]
        ),
        axis=1,
    )

    df = df[
        [
            "application_id",
            "job_id",
            "candidate_id",
            "apply_date",
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
            print(f"Skipping raw.applications; batch already loaded | batch_id={batch_id}")
            return

        con.register("apps_df", df)
        start_batch(
            con,
            batch_id,
            source_name,
            source_system,
            file_name,
            checksum,
            "append_extract",
            ingestion_ts,
        )

        con.execute("""
            INSERT INTO raw.applications
            SELECT * FROM apps_df
        """)

        mark_batch_succeeded(con, batch_id, datetime.utcnow(), len(df))
    except Exception as exc:
        mark_batch_failed(con, batch_id, datetime.utcnow(), str(exc))
        raise

    print(f"Loaded {len(df)} rows into raw.applications | batch_id={batch_id}")

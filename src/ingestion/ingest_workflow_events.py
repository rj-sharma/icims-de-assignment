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


def ingest_workflow_events(con):
    file_name = "workflow_events.jsonl"
    file_path = data_file_path(file_name)
    print(f"Ingesting: {file_path}")

    df = pd.read_json(file_path, lines=True)

    ingestion_ts = datetime.utcnow()
    source_name = "workflow_events"
    source_system = "workflow_event_export"
    checksum = file_checksum(file_path)
    batch_id = deterministic_batch_id(source_name, file_name, checksum)

    event_ts = pd.to_datetime(df["event_timestamp"], errors="coerce")

    df["_event_id"] = df.apply(
        lambda row: stable_hash(
            [
                row["application_id"],
                row["old_status"],
                row["new_status"],
                row["event_timestamp"],
            ]
        ),
        axis=1,
    )
    df["_event_date"] = event_ts.dt.date
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
                row["old_status"],
                row["new_status"],
                row["event_timestamp"],
            ]
        ),
        axis=1,
    )

    df = df[
        [
            "_event_id",
            "application_id",
            "old_status",
            "new_status",
            "event_timestamp",
            "_event_date",
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
        # Events are append-only across batches, but rerunning the same file
        # should replace that batch instead of duplicating it.
        con.execute("DELETE FROM raw.workflow_events WHERE _batch_id = ?", [batch_id])
        start_batch(
            con,
            batch_id,
            source_name,
            source_system,
            file_name,
            checksum,
            "append_batch",
            ingestion_ts,
        )

        con.register("events_df", df)
        con.execute("""
            INSERT INTO raw.workflow_events
            SELECT * FROM events_df
        """)

        mark_batch_succeeded(con, batch_id, datetime.utcnow(), len(df))
    except Exception as exc:
        mark_batch_failed(con, batch_id, datetime.utcnow(), str(exc))
        raise

    print(f"Loaded {len(df)} rows into raw.workflow_events | batch_id={batch_id}")

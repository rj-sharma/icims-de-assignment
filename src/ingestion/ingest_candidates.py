import json
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


def ingest_candidates(con):
    file_name = "candidates.json"
    file_path = data_file_path(file_name)
    print(f"Ingesting: {file_path}")

    with open(file_path) as file:
        data = json.load(file)

    df = pd.DataFrame(data)

    ingestion_ts = datetime.utcnow()
    source_name = "candidates"
    source_system = "candidate_profile_snapshot"
    checksum = file_checksum(file_path)
    batch_id = deterministic_batch_id(source_name, file_name, checksum)

    if "skills" in df.columns:
        df["_skills_hash_input"] = df["skills"].apply(
            lambda value: ",".join(sorted(str(skill).strip() for skill in value))
            if isinstance(value, list)
            else ",".join(sorted(skill.strip() for skill in str(value).split(",")))
        )
        df["skills"] = df["skills"].apply(
            lambda value: ",".join(value) if isinstance(value, list) else str(value)
        )
    else:
        df["_skills_hash_input"] = ""

    normalized_email = df["email"].astype(str).str.strip().str.lower()
    normalized_phone = df["phone"].astype(str).str.strip()

    df["email_hash"] = normalized_email.apply(lambda value: stable_hash([value]))
    df["phone_hash"] = normalized_phone.apply(lambda value: stable_hash([value]))
    df["_ingestion_ts"] = ingestion_ts
    df["_ingestion_date"] = ingestion_ts.date()
    df["_batch_id"] = batch_id
    df["_source_system"] = source_system
    df["_file_name"] = file_name
    df["_source_file_checksum"] = checksum
    df["_record_hash"] = df.apply(
        lambda row: stable_hash(
            [
                row["candidate_id"],
                row["first_name"],
                row["last_name"],
                row["email"],
                row["phone"],
                row["_skills_hash_input"],
            ]
        ),
        axis=1,
    )

    df = df[
        [
            "candidate_id",
            "first_name",
            "last_name",
            "email",
            "email_hash",
            "phone",
            "phone_hash",
            "skills",
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
            print(f"Skipping raw.candidates; batch already loaded | batch_id={batch_id}")
            return

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

        con.register("candidates_df", df)
        con.execute("""
            INSERT INTO raw.candidates
            SELECT * FROM candidates_df
        """)

        mark_batch_succeeded(con, batch_id, datetime.utcnow(), len(df))
    except Exception as exc:
        mark_batch_failed(con, batch_id, datetime.utcnow(), str(exc))
        raise

    print(f"Loaded {len(df)} rows into raw.candidates | batch_id={batch_id}")

import hashlib
import os

import duckdb
import pandas as pd


DB_PATH = os.getenv("ICIMS_DB_PATH", "icims.duckdb")
DATA_DIR = os.getenv("ICIMS_DATA_DIR", "data")


def get_connection():
    print("DB PATH:", os.path.abspath(DB_PATH))
    return duckdb.connect(DB_PATH)


def data_file_path(file_name):
    return os.path.join(DATA_DIR, file_name)


def file_checksum(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def deterministic_batch_id(source_name, file_name, checksum):
    return hashlib.md5(
        f"{source_name}|{file_name}|{checksum}".encode("utf-8")
    ).hexdigest()


def stable_hash(values):
    joined = "||".join("" if pd.isna(value) else str(value) for value in values)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def start_batch(
    con,
    batch_id,
    source_name,
    source_system,
    file_name,
    checksum,
    load_strategy,
    started_at,
):
    con.execute("DELETE FROM raw.ingestion_batches WHERE batch_id = ?", [batch_id])
    con.execute(
        """
        INSERT INTO raw.ingestion_batches
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        """,
        [
            batch_id,
            source_name,
            source_system,
            file_name,
            checksum,
            load_strategy,
            "STARTED",
            started_at,
        ],
    )


def mark_batch_succeeded(con, batch_id, completed_at, rows_loaded):
    con.execute(
        """
        UPDATE raw.ingestion_batches
        SET status = 'SUCCEEDED',
            completed_at = ?,
            rows_loaded = ?
        WHERE batch_id = ?
        """,
        [completed_at, rows_loaded, batch_id],
    )


def mark_batch_failed(con, batch_id, completed_at, error_message):
    con.execute(
        """
        UPDATE raw.ingestion_batches
        SET status = 'FAILED',
            completed_at = ?,
            error_message = ?
        WHERE batch_id = ?
        """,
        [completed_at, error_message, batch_id],
    )

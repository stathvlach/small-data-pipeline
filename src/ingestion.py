import logging
import hashlib
import sqlite3
import pandas as pd
from config import LANDING_DIR
from database import execute_select, execute_dml

logger = logging.getLogger(__name__)

def is_dataset_ingested(
    conn: sqlite3.Connection,
    dataset_hash: str
) -> bool:
    q = """
    SELECT 1 FROM dataset_meta WHERE dataset_hash = ?
    """

    return bool(execute_select(conn, q, (dataset_hash,)))

def add_dataset_meta(
    conn: sqlite3.Connection,
    dataset_hash: str,
    filename: str,
) -> bool:
    q = """
    INSERT INTO dataset_meta (dataset_hash, filename)
    VALUES (?, ?)
    ON CONFLICT(dataset_hash)
    DO UPDATE SET
        filename = excluded.filename,
        last_seen = datetime('now')
    ;
    """

    return bool(execute_dml(conn, q, (dataset_hash, filename)))

def sense(
    tracking_conn: sqlite3.Connection,
) -> list[dict[str, str]]:
    logger.info(f"Sensing files from landing directory: {LANDING_DIR}\n")
    sensed_files = []
    for file in LANDING_DIR.iterdir():
        if file.is_file() and file.suffix.lower() == ".csv":
            dataset_hash = hashlib.sha256(file.read_bytes()).hexdigest()

            if is_dataset_ingested(tracking_conn, dataset_hash):
                print(f"The file {file.name} is already processed. Do you like to ingested it again?")
                ans = input("y/n: ")
                if ans.lower() == "y":
                    sensed_files.append({
                        "hash":dataset_hash,
                        "table": file.stem.lower(),
                        "file_name": file.name
                    })
            else:
                sensed_files.append({
                    "hash":dataset_hash,
                    "table": file.stem.lower(),
                    "file_name": file.name
                })

    if not len(sensed_files):
        logger.info("The landing directory containes no new files. BYE")

    return sensed_files

def load_dataset_to_bronze(
    bronze_conn: sqlite3.Connection,
    df: pd.DataFrame,
    dataset: dict
) -> int:
    df.insert(0, "dataset_hash", dataset["hash"])
    df.insert(1, "record_id", range(1, df.shape[0] + 1))

    if dataset['table'] == "employees":
        q = """
        INSERT INTO employees (
            dataset_hash,
            record_id,
            employee_id,
            name,
            role
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(dataset_hash, record_id)
        DO UPDATE SET
            employee_id = excluded.employee_id,
            name = excluded.name,
            role = excluded.role,
            timestamp = CURRENT_TIMESTAMP;
        """
        rows = [
            (
                r["dataset_hash"],
                r["record_id"],
                r["employee_id"],
                r["name"],
                r["role"]
            )
            for r in df.to_dict(orient="records")
        ]
    elif dataset['table'] == "projects":
        q = """
        INSERT INTO projects (
            dataset_hash,
            record_id,
            project_id,
            project_name,
            budget
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(dataset_hash, record_id)
        DO UPDATE SET
            project_id = excluded.project_id,
            project_name = excluded.project_name,
            budget = excluded.budget,
            timestamp = CURRENT_TIMESTAMP;
        """
        rows = [
            (
                r["dataset_hash"],
                r["record_id"],
                r["project_id"],
                r["project_name"],
                r["budget"]
            )
            for r in df.to_dict(orient="records")
        ]
    elif dataset['table'] == "timesheets":
        q = """
        INSERT INTO timesheets (
            dataset_hash,
            record_id,
            employee_id,
            project_id,
            date,
            hours
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(dataset_hash, record_id)
        DO UPDATE SET
            employee_id = excluded.employee_id,
            project_id = excluded.project_id,
            date = excluded.date,
            hours = excluded.hours,
            timestamp = CURRENT_TIMESTAMP;
        """
        rows = [
            (
                r["dataset_hash"],
                r["record_id"],
                r["employee_id"],
                r["project_id"],
                r["date"],
                r["hours"]
            )
            for r in df.to_dict(orient="records")
        ]
    else:
        raise ValueError(f"Unkonwn table/dataset: {dataset['table']}")

    try:
        bronze_conn.executemany(q, rows)
        bronze_conn.commit()
        logger.info("%d obs loaded from dataset %s into %s", len(df), dataset["file_name"], dataset["table"])
        return len(df)
    except Exception:
        logger.exception("Failed loading dataset %s into %s", dataset["file_name"], dataset["table"])
        raise

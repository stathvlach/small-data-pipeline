import logging
import sqlite3
import re
import pandas as pd
from config import TRACKING_DB_PATH
from database import execute_select

logger = logging.getLogger(__name__)

def load_bronze_dataset(
    bronze_conn: sqlite3.Connection,
    table: str,
    dataset_hash: str,
) -> pd.DataFrame:

    bronze_conn.execute(f"ATTACH DATABASE '{TRACKING_DB_PATH}' AS tracking;")

    q = f"""
    SELECT *
    FROM {table}
    WHERE dataset_hash = ?
    AND record_id NOT IN (
        SELECT record_id
        FROM tracking.data_quality
        WHERE severity = 'ERROR'
    );
    """

    df = pd.read_sql_query(q, bronze_conn, params=[dataset_hash])
    bronze_conn.execute("DETACH DATABASE tracking;")
    return df

def trim_whitespaces(df: pd.DataFrame, fields: tuple[str, ...]) -> pd.DataFrame:
    df = df.copy()

    for field in fields:
        df[field] = df[field].str.strip()

    return df

def normalize_whitespaces(df: pd.DataFrame, fields: tuple[str, ...]) -> pd.DataFrame:
    df = df.copy()

    for field in fields:
        df[field] = df[field].apply(
            lambda x: re.sub(r"\s+", " ", x).strip() if isinstance(x, str) else x
        )

    return df

def normalize_case(df: pd.DataFrame, fields: tuple[str, ...], mode: str = "upper") -> pd.DataFrame:
    df = df.copy()

    for field in fields:
        if mode == "upper":
            df[field] = df[field].str.upper()
        elif mode == "lower":
            df[field] = df[field].str.lower()
        elif mode == "title":
            df[field] = df[field].str.title()
        else:
            raise ValueError(f"Unsupported case normalization mode: {mode}")

    return df

def convert_to_numeric(df: pd.DataFrame, fields: tuple[str, ...]) -> pd.DataFrame:
    df = df.copy()

    for field in fields:
        df[field] = pd.to_numeric(df[field], errors="coerce")

    return df.dropna(subset=fields)

def standarize_dates(df: pd.DataFrame, fields: tuple[str, ...]) -> pd.DataFrame:
    df = df.copy()

    for field in fields:
        df[field] = (pd.to_datetime(df[field], errors="raise")).dt.strftime("%d-%m-%Y")

    return df

def load_dataset_to_silver(
    silver_conn: sqlite3.Connection,
    df: pd.DataFrame,
    table: str
) -> int:
    if table == "employees":
        q = """
        INSERT INTO employees (employee_id, name, role)
        VALUES (?, ?, ?)
        ON CONFLICT(employee_id)
        DO UPDATE SET
            name = excluded.name,
            role = excluded.role,
            timestamp = CURRENT_TIMESTAMP;
        """
        rows = [
            (r["employee_id"], r["name"], r["role"]) for r in df.to_dict(orient="records")
        ]

    elif table == "projects":
        q = """
        INSERT INTO projects (project_id, project_name, budget)
        VALUES (?, ?, ?)
        ON CONFLICT(project_id)
        DO UPDATE SET
            project_name = excluded.project_name,
            budget = excluded.budget,
            timestamp = CURRENT_TIMESTAMP;
        """
        rows = [
            (r["project_id"], r["project_name"], r["budget"]) for r in df.to_dict(orient="records")
        ]

    elif table == "timesheets":
        silver_conn.execute("PRAGMA foreign_key_check;")
        q = """
        INSERT INTO timesheets (employee_id, project_id, date, hours)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(employee_id, project_id, date)
        DO UPDATE SET
            hours = excluded.hours,
            timestamp = CURRENT_TIMESTAMP;
        """
        rows = [
            (r["employee_id"], r["project_id"], r["date"], r["hours"]) for r in df.to_dict(orient="records")
        ]
    else:
        raise ValueError(f"Unkonwn table/dataset: {table}")

    try:
        silver_conn.executemany(q, rows)
        silver_conn.commit()
        logger.info("%d obs loaded in dataset into %s", len(df), table)
        return len(df)
    except Exception:
        logger.exception("Failed loading dataset into %s", table)
        raise

def clean_employees(
    bronze_conn: sqlite3.Connection,
    dataset_hash: str
) -> tuple[pd.DataFrame, pd.Series]:
    df = load_bronze_dataset(bronze_conn, "employees", dataset_hash)

    df = trim_whitespaces(df, ("employee_id", "name", "role"))
    df = normalize_whitespaces(df, ("employee_id", "name", "role"))
    df = normalize_case(df, ("role",), "title")

    null_mask = df.isna().any(axis=1)

    accepted = pd.DataFrame(df.loc[~null_mask].drop(columns=["dataset_hash", "record_id"]))
    rejected = pd.Series(df.loc[null_mask, "record_id"])

    return (accepted, rejected)

def clean_projects(
    bronze_conn: sqlite3.Connection,
    dataset_hash: str
) -> tuple[pd.DataFrame, pd.Series]:
    df = load_bronze_dataset(bronze_conn, "projects", dataset_hash)

    df = trim_whitespaces(df, ("project_id", "project_name"))
    df = normalize_whitespaces(df, ("project_id", "project_name"))
    df = convert_to_numeric(df, ("budget",))

    null_mask = df.isna().any(axis=1)

    accepted = pd.DataFrame(df.loc[~null_mask].drop(columns=["dataset_hash", "record_id"]))
    rejected = pd.Series(df.loc[null_mask, "record_id"])

    return (accepted, rejected)

def clean_timesheets(
    bronze_conn: sqlite3.Connection,
    silver_conn: sqlite3.Connection,
    dataset_hash: str
) -> tuple[pd.DataFrame, pd.Series]:
    df = load_bronze_dataset(bronze_conn, "timesheets", dataset_hash)

    q = """
    SELECT DISTINCT employee_id FROM employees ORDER BY employee_id;
    """
    rows = execute_select(silver_conn, q, ())
    unique_employee_ids = [row["employee_id"] for row in rows]

    q = """
    SELECT DISTINCT project_id FROM projects ORDER BY project_id;
    """
    rows = execute_select(silver_conn, q, ())
    unique_project_ids = [row["project_id"] for row in rows]

    fk_mask = (
        df["employee_id"].isin(unique_employee_ids)
        &
        df["project_id"].isin(unique_project_ids)
    )
    null_mask = df.isna().any(axis=1)

    df = df.loc[fk_mask].copy()

    df = trim_whitespaces(df, ("employee_id", "project_id", "date"))
    df = normalize_whitespaces(df, ("employee_id", "project_id", "date"))
    df = normalize_case(df, ("employee_id", "project_id", "date"))
    df = convert_to_numeric(df, ("hours",))

    accepted = df.loc[~null_mask].drop(columns=["dataset_hash", "record_id"])

    rejected = pd.Series(pd.concat([
        df.loc[~fk_mask, "record_id"],
        df.loc[null_mask, "record_id"]
    ]))

    return (accepted, rejected)

def clean(
    bronze_conn: sqlite3.Connection,
    silver_conn: sqlite3.Connection,
    dataset_hash: str,
    table: str
) -> tuple[pd.DataFrame, pd.Series]:

    if table == "employees":
        accepted, rejected = clean_employees(bronze_conn, dataset_hash)
    elif table == "projects":
        accepted, rejected = clean_projects(bronze_conn, dataset_hash)
    elif table == "timesheets":
        accepted, rejected = clean_timesheets(bronze_conn, silver_conn, dataset_hash)
    else:
        raise ValueError(f"Unkonwn table/dataset: {table}")

    return (accepted, rejected)

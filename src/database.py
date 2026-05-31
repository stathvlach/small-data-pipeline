import logging
import sqlite3
from pathlib import Path
from time import perf_counter

logger = logging.getLogger(f"{__name__}.sqlite")

def sql_connect(db_path: Path) -> sqlite3.Connection:
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.row_factory = sqlite3.Row
        return conn

    except sqlite3.OperationalError as e:
        err_msg = f"SQLite operational error: {e}"
        logger.error(err_msg)
        raise RuntimeError(err_msg) from e

    except sqlite3.DatabaseError as e:
        err_msg = f"SQLite database error (possibly corrupt DB): {e}"
        logger.error(err_msg)
        raise RuntimeError(err_msg) from e

    except Exception as e:
        err_msg = f"Unexpected error while connecting to SQLite: {e}"
        logger.error(err_msg)
        raise RuntimeError(err_msg) from e


def execute_sql_file(conn: sqlite3.Connection, sql_path: Path) -> bool:
    sql = sql_path.read_text()
    start = perf_counter()

    logger.info("Executing SQL file: %s", sql_path)

    try:
        with conn:
            conn.executescript(sql)

        elapsed = perf_counter() - start

        logger.info("SQL file executed successfully: %s | Elapsed time = %.3fs", sql_path, elapsed)

        return True
    except sqlite3.Error:
        elapsed = perf_counter() - start
        logger.exception("SQL file execution failed: %s | Elapsed time = %.3fs", sql_path, elapsed)
        return False

def execute_select(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple | None = None
) -> list[sqlite3.Row]:

    logger.info(f"Executing SELECT query with params: {params}")

    start = perf_counter()
    try:
        cursor = conn.execute(sql, params or ())
        rows = cursor.fetchall()
        elapsed = perf_counter() - start

        row_count = len(rows)

        logger.info("SELECT executed | rows=%d | elapsed=%.3fs", row_count, elapsed)

        return rows

    except sqlite3.Error:
        elapsed = perf_counter() - start
        logger.exception("SELECT query failed")
        return []

def execute_dml(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple | None = None
) -> int:

    logger.info(f"Executing DML query with params:{params}")

    start = perf_counter()

    try:
        with conn:
            cursor = conn.execute(sql, params or ())
            affected_rows = cursor.rowcount

        elapsed = perf_counter() - start

        logger.info(
            "DML executed successfully | affected_rows=%d | elapsed=%.3fs",
            affected_rows,
            elapsed
        )

        return affected_rows

    except sqlite3.Error:
        elapsed = perf_counter() - start
        logger.exception("DML execution failed")
        return 0

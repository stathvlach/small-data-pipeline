import logging
import sqlite3
import pandas as pd

logger = logging.getLogger(__name__)

def calculate_project_totals(
    silver_conn: sqlite3.Connection,
    gold_conn: sqlite3.Connection
) -> int:
    q = """
    SELECT project_id, SUM(hours) as total_hours
    FROM timesheets
    GROUP BY project_id
    ORDER BY project_id;
    """

    df = pd.read_sql_query(q, silver_conn)

    if not df.empty:
        df.to_sql(
            "total_hours_per_project",
            gold_conn,
            if_exists="replace",
            index=False
        )

    return len(df)

def calculate_employee_totals(
    silver_conn: sqlite3.Connection,
    gold_conn: sqlite3.Connection
) -> int:
    q = """
    SELECT employee_id, SUM(hours) as total_hours
    FROM timesheets
    GROUP BY employee_id
    ORDER BY employee_id;
    """

    df = pd.read_sql_query(q, silver_conn)

    if not df.empty:
        df.to_sql(
            "total_hours_per_employee",
            gold_conn,
            if_exists="replace",
            index=False
        )

    return len(df)

import logging
import sqlite3
import pandas as pd
from database import execute_select, execute_dml

logger = logging.getLogger(__name__)

def add_data_quality_entry(
    conn: sqlite3.Connection,
    table: str,
    dataset_hash: str,
    record_id: int,
    severity: str,
    issue: str
) -> int:
    q = """
    INSERT INTO data_quality (table_name, dataset_hash, record_id, severity, issue)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(table_name, dataset_hash, record_id)
    DO UPDATE SET
        severity = excluded.severity,
        issue = excluded.issue,
        timestamp = CURRENT_TIMESTAMP;
    """
    return execute_dml(conn, q, (table, dataset_hash, record_id, severity, issue))

def get_business_fields(
    conn: sqlite3.Connection,
    table: str
) -> tuple[str, ...]:

    q = f"""
    SELECT name
    FROM pragma_table_info('{table}')
    WHERE name NOT IN ('record_id', 'dataset_hash');
    """

    rows = execute_select(conn, q, ())

    return tuple(row["name"] for row in rows)

def check_null_obs(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str
) -> list[sqlite3.Row]:
    q = f"""
    SELECT record_id
    FROM {table}
    WHERE dataset_hash = ? AND {field} IS NULL;
    """

    res = execute_select(conn, q, (dataset_hash,))

    if res:
        logger.info(f"Found {len(res)} Null obs found in {table}.{field}")

    return res

def check_exact_duplicates(
    conn: sqlite3.Connection,
    table: str,
    dataset_hash: str
) -> list[sqlite3.Row]:

    fields = get_business_fields(conn, table)

    partition_sql = ", ".join(fields)

    q = f"""
    WITH ranked AS (
        SELECT
            record_id,
            ROW_NUMBER() OVER (
                PARTITION BY {partition_sql}
                ORDER BY record_id
            ) AS rn
        FROM {table}
        WHERE dataset_hash = ?
    )
    SELECT record_id
    FROM ranked
    WHERE rn > 1;
    """

    res = execute_select(conn, q, (dataset_hash,))

    if res:
        logger.info(f"Found {len(res)} exact duplicate obs in {table}")

    return res

def check_duplicate_obs(
    conn: sqlite3.Connection,
    table: str,
    fields: tuple[str, ...],
    dataset_hash: str
) -> list[sqlite3.Row]:

    fields_sql = ", ".join(fields)
    join_cond = " AND ".join([f"d.{f} = k.{f}" for f in fields])

    business_fields = get_business_fields(conn, table)
    business_fields_sql = ", ".join(business_fields)

    q = f"""
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {business_fields_sql}
                ORDER BY record_id
            ) AS rn
        FROM {table}
        WHERE dataset_hash = ?
    ),

    exact_duplicates AS (
        SELECT record_id
        FROM ranked
        WHERE rn > 1
    ),

    dedup AS (
        SELECT *
        FROM ranked
        WHERE rn = 1
    ),

    key_duplicates AS (
        SELECT d.record_id
        FROM dedup d
        JOIN (
            SELECT {fields_sql}
            FROM dedup
            GROUP BY {fields_sql}
            HAVING COUNT(*) > 1
        ) k
        ON {join_cond}
    )

    SELECT record_id
    FROM key_duplicates d
    WHERE NOT EXISTS (
        SELECT 1
        FROM exact_duplicates e
        WHERE e.record_id = d.record_id
    );
    """

    res = execute_select(conn, q, (dataset_hash,))

    if res:
        logger.info(
            "%d duplicate obs found in %s on (%s)",
            len(res),
            table,
            ", ".join(fields)
        )

    return res

def check_duplicate_obs_with_different_id(
    conn: sqlite3.Connection,
    table: str,
    table_id: str,
    field: str,
    dataset_hash: str
) -> list[sqlite3.Row]:
    q = f"""
    SELECT {table_id}
    FROM {table}
    WHERE dataset_hash = ?
      AND {field} IN (
        SELECT {field}
        FROM {table}
        WHERE dataset_hash = ?
        GROUP BY {field}
        HAVING COUNT(DISTINCT {table_id}) > 1
    );
    """

    res = execute_select(conn, q, (dataset_hash, dataset_hash))

    if res:
        logger.info(f"Found {len(res)} duplicate obs with different ids in {table}.{field}")

    return res

def check_value_pattern(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str,
    pattern: str
) -> list[dict[str, int]]:
    q = f"""
    SELECT record_id, {field}
    FROM {table}
    WHERE dataset_hash = ?;
    """

    df = pd.read_sql_query(q, conn, params=[dataset_hash])

    res = df.loc[
        ~df[field].astype(str).str.match(pattern, na=False),
        ["record_id"]
    ].to_dict("records")

    if res:
        logger.info(f"Found {len(res)} obs not matching pattern {pattern} in {table}.{field}")

    return res

def check_is_not_numeric(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str
) -> list[dict[str, int]]:
    q = f"""
    SELECT record_id, {field}
    FROM {table}
    WHERE dataset_hash = ?;
    """

    df = pd.read_sql_query(q, conn, params=[dataset_hash])

    num = pd.Series(pd.to_numeric(df[field], errors="coerce"))

    res = df.loc[num.isna(), ["record_id"]].to_dict("records")

    if res:
        logger.info(f"Found {len(res)} not numerical obs in {table}.{field}")

    return res

def check_numeric_out_of_range(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str,
    min_val: float,
    max_val: float
) -> list[dict[str, int]]:

    q = f"""
    SELECT record_id, {field}
    FROM {table}
    WHERE dataset_hash = ?;
    """

    df = pd.read_sql_query(q, conn, params=[dataset_hash])

    num = pd.Series(pd.to_numeric(df[field], errors="coerce"))

    res = df.loc[
        (num < min_val) | (num > max_val),
        ["record_id"]
    ].to_dict("records")

    if res:
        logger.info(f"Found {len(res)} out-of-range values in {table}.{field}")

    return res

def check_is_numeric_and_negative(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str
) -> list[dict[str, int]]:
    q = f"""
    SELECT record_id, {field}
    FROM {table}
    WHERE dataset_hash = ?;
    """

    df = pd.read_sql_query(q, conn, params=[dataset_hash])

    num = pd.Series(pd.to_numeric(df[field], errors="coerce"))

    res = df.loc[num.notna() & (num < 0), ["record_id"]].to_dict("records")

    if res:
        logger.info(f"Found {len(res)} negative obs in {table}.{field}")

    return res

def check_numeric_above_limit(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str,
    limit: float,
    bound: float
) -> list[dict[str, int]]:
    q = f"""
    SELECT record_id, {field}
    FROM {table}
    WHERE dataset_hash = ?;
    """

    df = pd.read_sql_query(q, conn, params=[dataset_hash])

    num = pd.Series(pd.to_numeric(df[field], errors="coerce"))

    res = df.loc[num.notna() & (num > limit) & (num <= bound), ["record_id"]].to_dict("records")

    if res:
        logger.info(f"Found {len(res)} obs above limit {limit} in {table}.{field}")

    return res

def check_unusual_diviation(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str,
    threshold: float
) -> list[dict[str, int]]:
    q = f"""
    SELECT record_id, {field}
    FROM {table}
    WHERE dataset_hash = ?;
    """

    df = pd.read_sql_query(q, conn, params=[dataset_hash])

    num = pd.Series(pd.to_numeric(df[field], errors="coerce"))

    mean = num.mean()

    res = df.loc[
        (num.notna()) & (abs(num - mean) / mean > threshold),
        ["record_id"]
    ].to_dict("records")

    if res:
        logger.info(f"Found {len(res)} obs with unusual deviation in {table}.{field} using threshold {threshold}")

    return res

def check_valid_date(
    conn: sqlite3.Connection,
    table: str,
    field: str,
    dataset_hash: str,
) -> list[dict[str, int]]:
    q = f"""
    SELECT record_id, {field}
    FROM {table}
    WHERE dataset_hash = ?;
    """

    df = pd.read_sql_query(q, conn, params=[dataset_hash])

    res = df.loc[
        pd.to_datetime(df["date"], errors="coerce", dayfirst=True).isna(),
        ["record_id"]
    ].to_dict("records")

    if res:
        logger.info(f"Found {len(res)} obs with not valid date in {table}.{field}")

    return res

def check_missing_foreign_keys(
    conn: sqlite3.Connection,
    table_l: str,
    table_r: str,
    field: str,
    dataset_hash: str
) -> list[sqlite3.Row]:

    q = f"""
    SELECT l.record_id
    FROM {table_l} l
    LEFT JOIN {table_r} r
        ON r.{field} = l.{field}
    WHERE r.{field} IS NULL AND l.dataset_hash = ?;
    """

    res = execute_select(conn, q, (dataset_hash,))

    if res:
        logger.info(f"Found {len(res)} missing keys in {table_r}.{field}")

    return res

def validate_employees(
    bronze_conn: sqlite3.Connection,
    tracking_conn: sqlite3.Connection,
    dataset_hash: str
) -> int:
    logger.info(f"\n\nSTART 'employees' validation, dataset: {dataset_hash}\n")

    issues = 0
    # Check employee_id is NOT NULL
    rows = check_null_obs(bronze_conn, "employees", "employee_id", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "employees",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "employee_id is NULL"
        )

    # Check employee_id row uniqueness
    rows = check_exact_duplicates(bronze_conn, "employees", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "employees",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "employee exact row duplicate"
        )

    # Check employee_id keys uniqueness
    rows = check_duplicate_obs(bronze_conn, "employees", ("employee_id",), dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "employees",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "employee_id duplicates"
        )

    # Check name is NOT NULL
    rows = check_null_obs(bronze_conn, "employees", "name", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "employees",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "name is NULL"
        )
    # Check role is not NULL
    rows = check_null_obs(bronze_conn, "employees", "role", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "employees",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "role is NULL"
        )

    # Check employee_id follows pattern "E\d(3)"
    rows = check_value_pattern(bronze_conn, "employees", "employee_id", dataset_hash, r"^E\d{3}$")
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "employees",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "employee_id naming pattern is not correct"
        )

    logger.info(f"\n\nFINISHED 'employees' validation, dataset: {dataset_hash}\n")
    return issues

def validate_projects(
    bronze_conn: sqlite3.Connection,
    tracking_conn: sqlite3.Connection,
    dataset_hash: str
) -> int:
    logger.info(f"\n\nSTART 'projects' validation, dataset: {dataset_hash}\n")
    issues = 0
    # Check project_id is NOT NULL
    rows = check_null_obs(bronze_conn, "projects", "project_id", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "project_id is NULL"
        )

    # Check projects row uniqueness
    rows = check_exact_duplicates(bronze_conn, "projects", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "projects exact duplicates"
        )

    # Check project_id keys uniqueness
    rows = check_duplicate_obs(bronze_conn, "projects", ("project_id",), dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "project_id duplicates"
        )

    # Check project_name is NOT NULL
    rows = check_null_obs(bronze_conn, "projects", "project_name", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "project_name is NULL"
        )

    # Check budget is NOT NULL
    rows = check_null_obs(bronze_conn, "projects", "budget", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "budget is NULL"
        )

    # Check project_id follows pattern "P\d(3)" (ERROR, SQL)
    rows = check_value_pattern(bronze_conn, "projects", "project_id", dataset_hash, r"^P\d{3}$")
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "project_id naming pattern is not correct"
        )

    # Check budget is numeric
    rows = check_is_not_numeric(bronze_conn, "projects", "budget", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "budget is not numeric"
        )

    # Check budget is greater than zero
    rows = check_is_numeric_and_negative(bronze_conn, "projects", "budget", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "budget is negative"
        )

    # Check for unusually high or low budgets compared to dataset distribution
    rows = check_unusual_diviation(bronze_conn, "projects", "budget", dataset_hash, 0.6)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "budget diviates a lot from mean"
        )

    # Check for duplicated project names with different IDs
    rows = check_duplicate_obs_with_different_id(bronze_conn, "projects", "project_id","project_name", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "projects",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "project_name duplicates with different ids"
        )

    logger.info(f"\n\nFINISHED 'projects' validation, dataset: {dataset_hash}\n")
    return issues

def validate_timesheets(
    bronze_conn: sqlite3.Connection,
    tracking_conn: sqlite3.Connection,
    dataset_hash: str
) -> int:
    logger.info(f"\n\nSTART 'timesheets' validation, dataset: {dataset_hash}\n")
    issues = 0
    # Check employee_id is NOT NULL
    rows = check_null_obs(bronze_conn, "timesheets", "employee_id", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "employee_id is NULL"
        )

    # Check project_id is NOT NULL
    rows = check_null_obs(bronze_conn, "timesheets", "project_id", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "project_id is NULL"
        )

    # Check date is NOT NULL
    rows = check_null_obs(bronze_conn, "timesheets", "date", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "date is NULL"
        )

    # Check hours is NOT NULL
    rows = check_null_obs(bronze_conn, "timesheets", "hours", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "hours is NULL"
        )

    # Check employee_id exists in employees dataset
    rows = check_missing_foreign_keys(bronze_conn, "timesheets", "employees", "employee_id", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "employee_id reference is missing"
        )

    # Check project_id exists in projects dataset
    rows = check_missing_foreign_keys(bronze_conn, "timesheets", "projects", "project_id", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "project_id reference is missing"
        )

    # Check timesheets row uniqueness
    rows = check_exact_duplicates(bronze_conn, "timesheets", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "projects exact duplicates"
        )

    # Check employee_id,project_id,date keys uniqueness
    rows = check_duplicate_obs(bronze_conn, "timesheets", ("employee_id", "project_id", "date"), dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "employee_id,project_id,date duplicates"
        )

    # Check hours is numeric
    rows = check_is_not_numeric(bronze_conn, "timesheets", "hours", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "hours is not numeric"
        )

    # Check hours between 0 and 24
    rows = check_numeric_out_of_range(bronze_conn, "timesheets", "hours", dataset_hash, 0, 24)
    issues += len(rows)
    print("DEBUGGGGGGGG")
    print(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "ERROR",
            "hours out of range"
        )

    # Check date can be parsed to valid date
    rows = check_valid_date(bronze_conn, "timesheets", "date", dataset_hash)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "date is not valid"
        )

    # Check unusually high daily hours (>9)
    rows = check_numeric_above_limit(bronze_conn, "timesheets", "hours", dataset_hash, 10, 24)
    issues += len(rows)
    for row in rows:
        add_data_quality_entry(
            tracking_conn,
            "timesheets",
            dataset_hash,
            row["record_id"],
            "WARNING",
            "hours unusal value"
        )

    logger.info(f"\n\nFINISHED 'timesheets' validation, dataset: {dataset_hash}\n")
    return issues

def validate(
    bronze_conn: sqlite3.Connection,
    tracking_conn: sqlite3.Connection,
    dataset_hash: str,
    table: str
) -> int:
    if table == "employees":
        return validate_employees(bronze_conn, tracking_conn, dataset_hash)
    elif table == "projects":
        return validate_projects(bronze_conn, tracking_conn, dataset_hash)
    elif table == "timesheets":
        return validate_timesheets(bronze_conn, tracking_conn, dataset_hash)
    else:
        raise ValueError(f"Uknown dataset/table: {table}")

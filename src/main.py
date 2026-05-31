import logging
import sqlite3
import pandas as pd
from pathlib import Path

from database import sql_connect, execute_dml, execute_select
from config import configure_logging, BRONZE_DB_PATH, TRACKING_DB_PATH, SILVER_DB_PATH, LANDING_DIR, GOLD_DB_PATH

from init import run_init
from ingestion import sense, load_dataset_to_bronze, add_dataset_meta
from validation import validate, add_data_quality_entry
from cleaning import clean, load_dataset_to_silver
from aggregate import calculate_employee_totals, calculate_project_totals

logger = logging.getLogger(__name__)

def update_tracking(
    tracking_conn: sqlite3.Connection,
    run_id: int,
    step: str,
    source_type: str,
    source_name: str,
    obs: int,
    csv_dataset_hash: str,
):

    q = """
    INSERT INTO pipeline_tracker (
        run_id,
        step,
        source_type,
        source_name,
        obs,
        csv_dataset_hash
    ) VALUES (?, ?, ?, ?, ?, ?);
    """

    return execute_dml(tracking_conn, q, (run_id, step, source_type, source_name, obs, csv_dataset_hash))

def get_max_run_id(
   tracking_conn: sqlite3.Connection
):
    q = """
    SELECT max(run_id) as max_id FROM pipeline_tracker LIMIT 1;
    """
    res = execute_select(tracking_conn, q, ())

    value = res[0]["max_id"]

    return value + 1 if value is not None else 1

def main():

    run_init()

    configure_logging()

    bronze_conn = sql_connect(Path(BRONZE_DB_PATH))
    silver_conn = sql_connect(Path(SILVER_DB_PATH))
    gold_conn = sql_connect(Path(GOLD_DB_PATH))
    tracking_conn = sql_connect(Path(TRACKING_DB_PATH))

    logger.info("\n\nSTART Pipeline.\n")

    max_run_id = get_max_run_id(tracking_conn)
    print(f"\n\n\nMAD ID: {max_run_id}")

    datasets = sense(tracking_conn)

    for dataset in datasets:
        csv_df = pd.read_csv(LANDING_DIR / dataset["file_name"])
        update_tracking(
            tracking_conn,
            max_run_id,
            'EXTRACT CSV',
            'CSV',
            dataset['file_name'],
            len(csv_df),
            dataset["hash"],
        )
        if not csv_df.empty:
            bronze_obs = load_dataset_to_bronze(bronze_conn, csv_df, dataset)
            update_tracking(
                tracking_conn,
                max_run_id,
                'LOAD TO BRONZE',
                'SQL',
                dataset['table'],
                bronze_obs,
                dataset["hash"],
            )
            add_dataset_meta(tracking_conn, dataset["hash"], dataset['file_name'])
            if bronze_obs:
                issues = validate(bronze_conn, tracking_conn, dataset["hash"], dataset['table'])
                update_tracking(
                    tracking_conn,
                    max_run_id,
                    'VALIDATION ISSUES',
                    'SQL',
                    dataset['table'],
                    issues,
                    dataset["hash"],
                )
                print(f"\n\nIssues fund: {issues} for '{dataset['table']}' table\n\n")
                accepted, rejected = clean(bronze_conn, silver_conn, dataset["hash"], dataset['table'])
                update_tracking(
                    tracking_conn,
                    max_run_id,
                    'CLEANING ACCEPTED',
                    'DF',
                    dataset['table'],
                    len(accepted),
                    dataset["hash"],
                )

                update_tracking(
                    tracking_conn,
                    max_run_id,
                    'CLEANING REJECTED',
                    'DF',
                    dataset['table'],
                    len(rejected),
                    dataset["hash"],
                )

                for record_id in rejected:
                    add_data_quality_entry(
                        tracking_conn,
                        dataset['table'],
                        dataset["hash"],
                        record_id,
                        "ERROR",
                        "clean failed"
                    )

                if len(accepted):
                    silver_obs = load_dataset_to_silver(silver_conn, accepted, dataset['table'])
                    update_tracking(
                        tracking_conn,
                        max_run_id,
                        'LOAD TO SILVER',
                        'SQL',
                        dataset['table'],
                        silver_obs,
                        dataset["hash"],
                    )

        obs_employee_totals= calculate_employee_totals(silver_conn, gold_conn)
        logger.info(f"{obs_employee_totals} obs added to total_hours_per_employee table.")

        obs_project_totals = calculate_project_totals(silver_conn, gold_conn)
        logger.info(f"{obs_project_totals} obs added to total_hours_per_project table.")

    bronze_conn.close()
    silver_conn.close()
    gold_conn.close()
    tracking_conn.close()

    return

if __name__ == "__main__":
    main()

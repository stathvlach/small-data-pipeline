import logging
from pathlib import Path
from config import BRONZE_DB_PATH, SILVER_DB_PATH, TRACKING_DB_PATH, GOLD_DB_PATH
from database import sql_connect, execute_sql_file

logger = logging.getLogger(__name__)

def run_init():

    Path("./db").mkdir(parents=True, exist_ok=True)
    Path("./logs").mkdir(parents=True, exist_ok=True)

    logger.info("Initialize databases.")

    if not Path(TRACKING_DB_PATH).exists():
        logger.info("Creating TRACKING layer database...")

        conn = sql_connect(Path(TRACKING_DB_PATH))

        if execute_sql_file(conn, Path("./sql/TRACKING_CREATE_TABLES.sql")):
            logger.info("Tracking layer database created.")
        else:
            logger.info("Tracking layer database creation failed.")

        conn.close()
    else:
        logger.info("Tracking layer database already exist.")

    if not Path(BRONZE_DB_PATH).exists():
        logger.info("Creating BRONZE layer database...")

        conn = sql_connect(Path(BRONZE_DB_PATH))

        res = execute_sql_file(conn, Path("./sql/BRONZE_CREATE_TABLES.sql"))

        if res:
            logger.info("Bronze layer database created.")
        else:
            logger.info("Bronze layer database creation failed.")

        conn.close()
    else:
        logger.info("Bronze layer database already exist.")

    if not Path(SILVER_DB_PATH).exists():
        logger.info("Creating SILVER layer database...")

        conn = sql_connect(Path(SILVER_DB_PATH))

        if execute_sql_file(conn, Path("./sql/SILVER_CREATE_TABLES.sql")):
            logger.info("Silver layer database created.")
        else:
            logger.info("Silver layer database creation failed.")

        conn.close()
    else:
        logger.info("Silver layer database already exist.")

    if not Path(GOLD_DB_PATH).exists():
        logger.info("Creating GOLD layer database...")

        conn = sql_connect(Path(GOLD_DB_PATH))

        if execute_sql_file(conn, Path("./sql/GOLD_CREATE_TABLES.sql")):
            logger.info("Gold layer database created.")
        else:
            logger.info("Gold layer database creation failed.")

        conn.close()
    else:
        logger.info("Gold layer database already exist.")

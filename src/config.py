import logging
from pathlib import Path
from datetime import datetime

LOG_DIR = Path("./logs")
LANDING_DIR = Path("./input")
TRACKING_DB_PATH = "./db/tracking.db"
BRONZE_DB_PATH = "./db/bronze.db"
SILVER_DB_PATH = "./db/silver.db"
GOLD_DB_PATH = "./db/gold.db"

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / f"PIPELINE_{datetime.now():%Y_%m_%d_%H_%M_%S}.log"),
            logging.StreamHandler(),
        ]
    )

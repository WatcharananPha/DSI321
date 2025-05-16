from pathlib import Path
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LOG_DIR = BASE_DIR / "logs"
DATA_OUTPUT_DIR_CONTAINER = BASE_DIR / "data_output"
EGAT_LOCAL_TEMP_DATA_DIR = DATA_OUTPUT_DIR_CONTAINER / "egat_temp"
EGAT_RAW_CSV_FILENAME = "egat_realtime_power_data.csv"
EGAT_LOCAL_RAW_CSV_FILE = EGAT_LOCAL_TEMP_DATA_DIR / EGAT_RAW_CSV_FILENAME
EGAT_SCRAPE_URL = os.getenv("EGAT_URL", "https://www.sothailand.com/sysgen/egat/")

LAKEFS_ENDPOINT_URL = os.getenv("LAKEFS_ENDPOINT_URL")
LAKEFS_ACCESS_KEY_ID = os.getenv("LAKEFS_ACCESS_KEY_ID")
LAKEFS_SECRET_ACCESS_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY")
LAKEFS_REPOSITORY = os.getenv("LAKEFS_REPOSITORY")
LAKEFS_BRANCH = os.getenv("LAKEFS_BRANCH")
LAKEFS_EGAT_DATA_PATH_PREFIX = os.getenv("LAKEFS_EGAT_DATA_PATH_PREFIX", "raw_data/egat/")

def ensure_dirs_exist():
    EGAT_LOCAL_TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

def get_lakefs_client_config():
    if not all([LAKEFS_ENDPOINT_URL, LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY, LAKEFS_REPOSITORY, LAKEFS_BRANCH]):
        raise ValueError("Missing lakeFS configuration")
    return {
        "endpoint": LAKEFS_ENDPOINT_URL,
        "access_key_id": LAKEFS_ACCESS_KEY_ID,
        "secret_access_key": LAKEFS_SECRET_ACCESS_KEY,
    }
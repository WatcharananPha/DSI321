from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.resolve()
LOCAL_DATA_DIR = PROJECT_ROOT / "data_temp"
LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
LAKEFS_BASE_PATH = "s3://your-lakefs-bucket/"
LAKEFS_RAW_EGAT_PATH_PREFIX = "raw/egat_realtime"
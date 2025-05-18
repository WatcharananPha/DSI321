import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
LAKEFS_STORAGE_DIR = DATA_DIR / "lakefs_storage"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUT_DATA_DIR = DATA_DIR / "output"

SRC_DIR = PROJECT_ROOT / "src"
FLOWS_DIR = SRC_DIR / "flows"
FRONTEND_DIR = SRC_DIR / "frontend"
BACKEND_DIR = SRC_DIR / "backend"

CONFIG_DIR = PROJECT_ROOT / "config"
DOCKER_CONFIG_DIR = CONFIG_DIR / "docker"
LOG_CONFIG_FILE = CONFIG_DIR / "logging.ini"

MODELS_DIR = PROJECT_ROOT / "models"
TRAINED_MODELS_DIR = MODELS_DIR / "trained"
MODEL_OUTPUTS_DIR = MODELS_DIR / "outputs"

LOGS_DIR = PROJECT_ROOT / "logs"
APP_LOG_FILE = LOGS_DIR / "app.log"
PIPELINE_LOG_FILE = LOGS_DIR / "pipeline.log"

ENV_FILE = PROJECT_ROOT / ".env"

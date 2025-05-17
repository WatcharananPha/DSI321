import os
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
import pandas as pd

from src.backend.scraping.scraper_egat import EGATRealTimeScraper
from src.backend.validation.validate import validate_data
from src.backend.load.lakefs_loader import LakeFSLoader

LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://localhost:8000/api/v1")
LAKEFS_ACCESS_KEY_ID = os.getenv("LAKEFS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
LAKEFS_SECRET_ACCESS_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
LAKEFS_REPO_NAME = os.getenv("LAKEFS_REPO_NAME", "egat-data-repo")
EGAT_URL = "https://www.sothailand.com/sysgen/egat/"
TARGET_BRANCH_LAKEFS = "main"

@task(name="Initialize EGAT Scraper", retries=1, retry_delay_seconds=5)
def init_scraper_task(url: str) -> EGATRealTimeScraper:
    return EGATRealTimeScraper(url=url)

@task(name="Scrape EGAT Data", retries=2, retry_delay_seconds=30, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10))
def scrape_egat_task(scraper: EGATRealTimeScraper) -> pd.DataFrame:
    scraper._initialize_driver()
    df = scraper.scrape_once()
    return df

@task(name="Validate Scraped Data", retries=1)
def validate_data_task(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return validate_data(df)

@task(name="Load Data to LakeFS as Parquet", retries=1, retry_delay_seconds=15)
def load_to_lakefs_task(df: pd.DataFrame, run_timestamp: datetime):
    if df.empty:
        return None
    loader = LakeFSLoader(
        repository_name=LAKEFS_REPO_NAME,
        lakefs_endpoint=LAKEFS_ENDPOINT,
        access_key_id=LAKEFS_ACCESS_KEY_ID,
        secret_access_key=LAKEFS_SECRET_ACCESS_KEY
    )
    ingest_branch_name = f"ingest/egat-parquet-{run_timestamp.strftime('%Y%m%d-%H%M%S-%f')}"
    file_date_folder = run_timestamp.strftime('%Y%m%d')
    file_time_stamp = run_timestamp.strftime('%H%M%S_%f')
    remote_parquet_path = f"raw/egat_realtime_parquet/{file_date_folder}/egat_data_{file_time_stamp}.parquet"
    uploaded_file_path = loader.upload_dataframe_as_parquet(df, branch=ingest_branch_name, remote_path=remote_parquet_path)
    if uploaded_file_path:
        commit_message = f"Add EGAT realtime data as Parquet, scraped at {run_timestamp.isoformat()}"
        metadata = {
            'source_url': EGAT_URL,
            'scraper_version': '1.1_parquet',
            'flow_run_id': os.getenv('PREFECT_FLOW_RUN_ID', 'N/A'),
            'file_format': 'parquet'
        }
        commit_id = loader.commit_changes(branch=ingest_branch_name, commit_message=commit_message, metadata=metadata)
        return commit_id
    return None

@task(name="Close Scraper Driver")
def close_scraper_driver_task(scraper: EGATRealTimeScraper):
    scraper.close_driver()

@flow(name="EGAT Realtime Parquet Ingestion Flow", log_prints=True)
def egat_data_ingestion_flow():
    current_run_timestamp = datetime.now()
    scraper_instance = init_scraper_task(url=EGAT_URL)
    raw_df = scrape_egat_task(scraper=scraper_instance)
    if raw_df.empty:
        return {"status": "No data", "commit_id": None}
    validated_df = validate_data_task(raw_df)
    if validated_df.empty:
        return {"status": "No valid data", "commit_id": None}
    commit_id = load_to_lakefs_task(validated_df, run_timestamp=current_run_timestamp)
    close_scraper_driver_task(scraper=scraper_instance)
    if commit_id:
        return {"status": "Success", "commit_id": commit_id}
    return {"status": "Partial success, no commit", "commit_id": None}

if __name__ == "__main__":
    egat_data_ingestion_flow()
import os
from prefect import flow, task
from datetime import datetime
import pandas as pd
from src.backend.scraping.scraper_egat import EGATRealTimeScraper
from src.backend.validation.validate import validate_data
from src.backend.load.lakefs_loader import LakeFSLoader

LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://localhost:8000/api/v1")
LAKEFS_ACCESS_KEY_ID = os.getenv("LAKEFS_ACCESS_KEY_ID", "ACCESS_KEY")
LAKEFS_SECRET_ACCESS_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "SECRET_KEY")
LAKEFS_REPO_NAME = os.getenv("LAKEFS_REPO_NAME", "egat-data-repo")
EGAT_URL = "https://www.sothailand.com/sysgen/egat/"

@task
def scrape_data() -> pd.DataFrame:
    scraper = EGATRealTimeScraper(url=EGAT_URL)
    scraper._initialize_driver()
    df = scraper.scrape_once()
    scraper.close_driver()
    return df

@task
def validate_scraped_data(df: pd.DataFrame) -> pd.DataFrame:
    return validate_data(df) if not df.empty else df

@task
def load_data(df: pd.DataFrame, timestamp: datetime):
    if df.empty:
        return None
        
    loader = LakeFSLoader(
        repository_name=LAKEFS_REPO_NAME,
        lakefs_endpoint=LAKEFS_ENDPOINT,
        access_key_id=LAKEFS_ACCESS_KEY_ID,
        secret_access_key=LAKEFS_SECRET_ACCESS_KEY
    )

    branch_name = f"ingest/egat-{timestamp.strftime('%Y%m%d-%H%M%S')}"
    remote_path = f"raw/egat_realtime/data_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
    
    uploaded_path = loader.upload_dataframe(df, branch=branch_name, remote_path=remote_path)
    if uploaded_path:
        return loader.commit_changes(
            branch=branch_name,
            commit_message=f"Add EGAT data {timestamp.isoformat()}",
            metadata={'source_url': EGAT_URL}
        )
    return None

@flow
def egat_data_ingestion_flow():
    timestamp = datetime.now()
    raw_df = scrape_data()
    if not raw_df.empty:
        validated_df = validate_scraped_data(raw_df)
        if not validated_df.empty:
            commit_id = load_data(validated_df, timestamp)
            return {"status": "Success", "commit_id": commit_id} if commit_id else {"status": "No commit", "commit_id": None}
    return {"status": "No data", "commit_id": None}

if __name__ == "__main__":
    egat_data_ingestion_flow()
from prefect import flow, task
import os
from src.backend.scraping.egat_scraper import EGATRealTimeScraper
from src.backend.load.lakefs_uploader import upload_and_commit_data_to_lakefs
from config.path_config import (
    EGAT_LOCAL_RAW_CSV_FILE,
    EGAT_RAW_CSV_FILENAME,
    ensure_dirs_exist
)

@task(name="Scrape EGAT Real-Time Data Task", retries=2, retry_delay_seconds=60)
def scrape_egat_data_task():
    ensure_dirs_exist()
    scraper = EGATRealTimeScraper()
    local_temp_csv_path = scraper.scrape_once()
    scraper.close()
    return local_temp_csv_path

@task(name="Upload and Commit to lakeFS Task", retries=1, retry_delay_seconds=30)
def upload_to_lakefs_task(local_temp_csv_path: str):
    if not local_temp_csv_path or not os.path.exists(local_temp_csv_path):
        raise FileNotFoundError(f"Required local CSV file not found: {local_temp_csv_path}")

    commit_meta = {
        "task_run_id": os.getenv("PREFECT_TASK_RUN_ID", "N/A"),
        "flow_run_id": os.getenv("PREFECT_FLOW_RUN_ID", "N/A"),
        "original_temp_file": local_temp_csv_path
    }

    commit_id = upload_and_commit_data_to_lakefs(
        local_file_path=local_temp_csv_path,
        lakefs_object_name=EGAT_RAW_CSV_FILENAME,
        commit_message_prefix="Automated EGAT Data Ingestion",
        commit_metadata=commit_meta
    )

    if commit_id:
        os.remove(local_temp_csv_path)
    
    return commit_id

@flow(name="EGAT Real-Time Data Ingestion Pipeline")
def egat_ingestion_pipeline_flow():
    temp_csv_path = scrape_egat_data_task()
    if temp_csv_path:
        return upload_to_lakefs_task(local_temp_csv_path=temp_csv_path)

if __name__ == "__main__":
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule

    egat_pipeline_deployment = Deployment.build_from_flow(
        flow=egat_ingestion_pipeline_flow,
        name="Scheduled EGAT Data Ingestion",
        description="Periodically scrapes EGAT real-time data and versions it in lakeFS.",
        version="1.0.0",
        tags=["egat", "scraping", "lakefs", "data-ingestion", "realtime"],
        work_pool_name="x-worker",
        schedule=CronSchedule(cron="*/5 * * * *", timezone="Asia/Bangkok")
    )

    egat_pipeline_deployment.apply()
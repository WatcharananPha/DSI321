import time
import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import os
import re
from prefect import task, flow, get_run_logger
from dotenv import load_dotenv
import lakefs_sdk
from lakefs_sdk.api import commits_api
from lakefs_sdk.model.commit_creation import CommitCreation

load_dotenv()

LAKEFS_ACCESS_KEY_ID = os.getenv("ACCESS_KEY")
LAKEFS_SECRET_ACCESS_KEY = os.getenv("SECRET_KEY")
LAKEFS_ENDPOINT_URL = os.getenv("LAKEFS_ENDPOINT", "http://lakefsdb:8000")
LAKEFS_REPOSITORY = os.getenv("LAKEFS_REPOSITORY", "egatdata")
LAKEFS_BRANCH = os.getenv("LAKEFS_BRANCH", "main")
LAKEFS_FILE_PATH = os.getenv("LAKEFS_PATH_PARQUET", "egat_realtime_power.parquet")

lakefs_storage_options = {
    "key": LAKEFS_ACCESS_KEY_ID,
    "secret": LAKEFS_SECRET_ACCESS_KEY,
    "client_kwargs": {"endpoint_url": LAKEFS_ENDPOINT_URL},
}
lakefs_s3_path = f"s3a://{LAKEFS_REPOSITORY}/{LAKEFS_BRANCH}/{LAKEFS_FILE_PATH}"

class EGATRealTimeScraper:
    def __init__(self, url="https://www.sothailand.com/sysgen/egat/"):
        self.url = url
        self.driver = None
        self.logger = get_run_logger()
        self._initialize_driver()

    def _initialize_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--log-level=0")
        chrome_options.set_capability('goog:loggingPrefs', {'browser': 'ALL'})
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

    def extract_data_from_console(self):
        self.driver.get(self.url)
        time.sleep(10)
        logs = self.driver.get_log('browser')
        for log_entry in logs[::-1]:
            message = log_entry.get('message', '')
            if 'updateMessageArea:' in message:
                match = re.search(
                    r'updateMessageArea:\s*(\d+)\s*,\s*(\d{1,2}:\d{2})\s*,\s*([0-9,]+\.?\d*)\s*,\s*(\d+\.?\d*)',
                    message
                )
                if match:
                    return {
                        'scrape_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'display_date_id': match.group(1).strip(),
                        'display_time': match.group(2).strip(),
                        'current_value_MW': float(match.group(3).replace(',', '').strip()),
                        'temperature_C': float(match.group(4).strip())
                    }
        return None

    def scrape_once(self):
        return self.extract_data_from_console()

    def close(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

@task(retries=3, retry_delay_seconds=10)
def scrape_egat_data_task():
    scraper = EGATRealTimeScraper()
    data = scraper.scrape_once()
    scraper.close()
    return pd.DataFrame([data]) if data else pd.DataFrame()

@task
def save_data_to_lakefs_task(df_new: pd.DataFrame):
    if df_new.empty:
        return "No data to save"

    df_new.to_parquet(lakefs_s3_path, storage_options=lakefs_storage_options, index=False)

    configuration = lakefs_sdk.Configuration()
    configuration.host = LAKEFS_ENDPOINT_URL
    configuration.username = LAKEFS_ACCESS_KEY_ID
    configuration.password = LAKEFS_SECRET_ACCESS_KEY
    client = lakefs_sdk.ApiClient(configuration)
    commits = commits_api.CommitsApi(client)

    commit_creation = CommitCreation(
        message=f"Automated commit: Updated EGAT power data at {datetime.datetime.now()}",
        metadata={"source": "prefect_pipeline"}
    )

    commits.commit(
        repository=LAKEFS_REPOSITORY,
        branch=LAKEFS_BRANCH,
        commit_creation=commit_creation
    )
    return f"Data saved and committed. Rows: {len(df_new)}"

@task
def ensure_lakefs_repository_and_branch():
    configuration = lakefs_sdk.Configuration()
    configuration.host = LAKEFS_ENDPOINT_URL
    configuration.username = LAKEFS_ACCESS_KEY_ID
    configuration.password = LAKEFS_SECRET_ACCESS_KEY
    client = lakefs_sdk.ApiClient(configuration)
    repo_api = lakefs_sdk.RepositoriesApi(client)
    branch_api = lakefs_sdk.BranchesApi(client)

    try:
        repo_api.get_repository(LAKEFS_REPOSITORY)
    except:
        from lakefs_sdk.model.repository_creation import RepositoryCreation
        repo_creation = RepositoryCreation(
            name=LAKEFS_REPOSITORY,
            storage_namespace=f"local:///home/lakefs/{LAKEFS_REPOSITORY}",
            default_branch=LAKEFS_BRANCH,
            sample_data=False
        )
        repo_api.create_repository(repository_creation=repo_creation)

    if LAKEFS_BRANCH != "main":
        try:
            branch_api.get_branch(LAKEFS_REPOSITORY, LAKEFS_BRANCH)
        except:
            from lakefs_sdk.model.branch_creation import BranchCreation
            source_commit_id = branch_api.get_branch(LAKEFS_REPOSITORY, "main").commit_id
            branch_creation = BranchCreation(name=LAKEFS_BRANCH, source=source_commit_id)
            branch_api.create_branch(LAKEFS_REPOSITORY, branch_creation)

@flow(name="EGAT Realtime Data Pipeline")
def egat_pipeline():
    ensure_lakefs_repository_and_branch()
    scraped_data_df = scrape_egat_data_task()
    if not scraped_data_df.empty:
        save_data_to_lakefs_task(scraped_data_df)

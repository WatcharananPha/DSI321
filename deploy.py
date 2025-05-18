import datetime
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from prefect.infrastructure.process import Process
from src.flows.egat_pipeline import egat_data_pipeline

schedule = IntervalSchedule(interval=datetime.timedelta(minutes=2))

Deployment.build_from_flow(
    flow=egat_data_pipeline,
    name="EGAT Realtime Data Scraper",
    version="1.0",
    tags=["egat", "scraping", "lakefs"],
    schedule=schedule,
    work_pool_name="docker-pool",
    work_queue_name="default",
    path=".",
    entrypoint="src/flows/egat_flow.py:egat_data_pipeline"
).apply()

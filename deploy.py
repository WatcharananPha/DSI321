import datetime
from prefect import deploy, flow
from prefect.server.schemas.schedules import IntervalSchedule
from src.flows.egat_pipeline import egat_pipeline

schedule = IntervalSchedule(interval=datetime.timedelta(minutes=2))

if __name__ == "__main__":
    deploy(
        flow=egat_pipeline,
        name="EGAT Realtime Data Scraper",
        version="1.0",
        tags=["egat", "scraping", "lakefs"],
        schedules=[{"schedule": schedule, "active": True}],
        work_pool_name="default-agent-pool",
        work_queue_name="default",
    )

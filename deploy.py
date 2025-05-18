import asyncio
import datetime
from prefect.client.orchestration import get_client
from prefect.server.schemas.schedules import IntervalSchedule
from src.flows.egat_pipeline import egat_pipeline

async def deploy():
    client = await get_client()
    work_pool_name = "egat-process-pool"
    
    # Use create_work_pool_if_not_exists pattern
    try:
        await client.read_work_pool(work_pool_name)
    except:
        await client.create_work_pool(
            name=work_pool_name,
            type="process"
        )
    
    # Deploy the flow
    await egat_pipeline.to_deployment(
        name="EGAT Realtime Data Scraper",
        version="1.0",
        tags=["egat", "scraping", "lakefs"],
        schedules=[{"schedule": IntervalSchedule(interval=datetime.timedelta(minutes=2)), "active": True}],
        work_pool_name=work_pool_name,
        work_queue_name="default",
    ).apply()

if __name__ == "__main__":
    asyncio.run(deploy())
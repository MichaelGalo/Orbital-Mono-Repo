
from data_ingestion import ingest_API_data, ingest_exoplanets
from utils import add_query_params, handle_date_adjustment
from logger import setup_logging
from db_sync import db_sync
from datetime import datetime, timezone
import os
import time
from prefect.client.schemas.schedules import CronSchedule
from prefect import flow
logger = setup_logging()

@flow(name="pipeline_runner")
def pipeline_runner():
    tick = time.time()

    today = datetime.now(timezone.utc).date()
    start_date = handle_date_adjustment(today, years=5).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    minio_bucket = os.getenv("MINIO_BUCKET_NAME")

    nasa_donki_url = add_query_params(os.getenv("NASA_DONKI_API"), {
        "startDate": start_date,
        "endDate": end_date,
        "type": "all",
        "api_key": os.getenv("NASA_API_KEY")
    })
    nasa_apod_url = add_query_params(os.getenv("NASA_APOD_API"), {
        "api_key": os.getenv("NASA_API_KEY")
    })
    astronaut_url = add_query_params(os.getenv("THE_SPACE_DEVS_API"), {
        "in_space": "true",
        "is_human": "true"
    })

    nasa_donki_filename = "nasa_donki.parquet"
    nasa_exoplanets_filename = "nasa_exoplanets.parquet"
    astronaut_filename = "astronauts.parquet"
    nasa_apod_filename = "nasa_apod.parquet"

    ingest_API_data(nasa_apod_url, nasa_apod_filename, minio_bucket)
    ingest_API_data(nasa_donki_url, nasa_donki_filename, minio_bucket)
    ingest_API_data(astronaut_url, astronaut_filename, minio_bucket)
    ingest_exoplanets(nasa_exoplanets_filename, minio_bucket)


    tock = time.time() - tick
    logger.info(f"Data ingestion completed in {tock:.2f} seconds.")

    logger.info("Synchronizing Data to Database")
    db_sync()    

if __name__ == "__main__":
    pipeline_runner.serve(
        name="Pipeline_Runner",
        schedule=CronSchedule(
            cron="0 1 * * *",
            timezone="UTC"
        ),
        tags=["Pipeline"]
    )
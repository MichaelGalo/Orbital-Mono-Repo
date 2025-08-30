import os
import requests
from dotenv import load_dotenv
import polars as pl
import time
from utils import write_data_to_minio, process_astronaut_data, convert_dataframe_to_parquet, add_query_params
from db_sync import db_sync
from logger import setup_logging
from prefect import flow, task
from prefect.client.schemas.schedules import CronSchedule
from astroquery.ipac.nexsci.nasa_exoplanet_archive import NasaExoplanetArchive
logger = setup_logging()
load_dotenv()

@task(name="fetching_api_data")
def fetch_api_data(base_url):
    response = requests.get(base_url)
    response.raise_for_status()
    data = response.json()

    astro_base_url = os.getenv("THE_SPACE_DEVS_API")
    if astro_base_url not in base_url:
        result = pl.DataFrame(data)
        return result

    astronauts_dataframe = pl.DataFrame(data["results"])
    processed_astronaut_dataframe = process_astronaut_data(astronauts_dataframe)

    result = processed_astronaut_dataframe
    return result


@task(name="exoplanet_data_fetch")
def query_confirmed_planets():
    try:
        tick = time.time()
        logger.info("Querying confirmed exoplanets from NASA Exoplanet Archive")
        target_table = "pscomppars"
        all_planets = NasaExoplanetArchive.query_criteria(table=target_table, select="*")
        tock = time.time()
        logger.info(f"Query completed in {tock - tick:.2f} seconds")
        planets_df = all_planets.to_pandas()
        exoplanets_dataframe = pl.from_pandas(planets_df)
        exoplanets_parquet_buffer = convert_dataframe_to_parquet(exoplanets_dataframe)
        return exoplanets_parquet_buffer
    except Exception as e:
        logger.error(f"Query failed: {e}")

def ingest_and_store_exoplanets_data(output_file_name, minio_bucket):
    exoplanets_parquet_buffer = query_confirmed_planets()
    logger.info("Writing Exoplanets Data to MinIO Storage")
    write_data_to_minio(exoplanets_parquet_buffer, minio_bucket, output_file_name, "RAW")

def ingest_and_store_api_data(API_url, output_file_name, minio_bucket):
    logger.info(f"Fetching Data from API")
    api_dataframe = fetch_api_data(API_url)
    api_parquet_buffer = convert_dataframe_to_parquet(api_dataframe)
    logger.info("Writing API Data to MinIO Storage")
    write_data_to_minio(api_parquet_buffer, minio_bucket, output_file_name, "RAW")

@flow(name="data_ingestion_flow")
def data_ingestion():
    tick = time.time()
    minio_bucket = os.getenv("MINIO_BUCKET_NAME")

    nasa_donki_url = add_query_params(os.getenv("NASA_DONKI_API"), {
        "startDate": "2020-01-01",
        "endDate": "2025-07-31",
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
    nasa_apod_filename = "nasa_apod.parquet"
    nasa_exoplanets_filename = "nasa_exoplanets.parquet"
    astronaut_filename = "astronauts.parquet"

    ingest_and_store_exoplanets_data(nasa_exoplanets_filename, minio_bucket)
    ingest_and_store_api_data(nasa_donki_url, nasa_donki_filename, minio_bucket)
    ingest_and_store_api_data(nasa_apod_url, nasa_apod_filename, minio_bucket)
    ingest_and_store_api_data(astronaut_url, astronaut_filename, minio_bucket)

    tock = time.time() - tick
    logger.info(f"Data ingestion completed in {tock:.2f} seconds.")

    logger.info("Synchronizing Data to Database")
    db_sync()    

if __name__ == "__main__":
    data_ingestion.serve(
        name="Data_Ingestion",
        schedule=CronSchedule(
            cron="0 1 * * *",
            timezone="UTC"
        ),
        tags=["data_ingestion"]
    )
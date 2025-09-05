from utils import write_data_to_minio, process_astronaut_data, convert_dataframe_to_parquet, write_data_to_mongo, package_data_for_mongo
from logger import setup_logging
import time
import os
import requests
from dotenv import load_dotenv
import polars as pl
from prefect import task
from astroquery.ipac.nexsci.nasa_exoplanet_archive import NasaExoplanetArchive
logger = setup_logging()

load_dotenv()

def fetch_api_dataframe(base_url):
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

def fetch_api_data(base_url):
    response = requests.get(base_url)
    response.raise_for_status()
    data = response.json()
    result = data
    return result

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

@task(name="exoplanet_data_ingestion")
def ingest_exoplanets(output_file_name, minio_bucket):
    exoplanets_parquet_buffer = query_confirmed_planets()
    logger.info("Writing Exoplanets Data to MinIO Storage")
    write_data_to_minio(exoplanets_parquet_buffer, minio_bucket, output_file_name, "RAW")

@task(name="apod_data_ingestion")
def ingest_APOD(API_url, mongo_db, mongo_collection):
    mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")
    apod_data = fetch_api_data(API_url)
    upload = package_data_for_mongo(apod_data)
    write_data_to_mongo(upload, mongo_connection_string, mongo_db, mongo_collection)

@task(name="api_data_ingestion")
def ingest_API_data(API_url, output_file_name, minio_bucket):
    logger.info("Fetching Data from API")
    api_dataframe = fetch_api_dataframe(API_url)
    api_parquet_buffer = convert_dataframe_to_parquet(api_dataframe)
    logger.info("Writing API Data to MinIO Storage")
    write_data_to_minio(api_parquet_buffer, minio_bucket, output_file_name, "RAW")
import os
import requests
from dotenv import load_dotenv
import io
import polars as pl
import time
from utils import write_data_to_minio
from db_sync import db_sync
from logger import setup_logging
from astroquery.ipac.nexsci.nasa_exoplanet_archive import NasaExoplanetArchive
logger = setup_logging()
load_dotenv()


def fetch_api_data(base_url):
    limit = 50000
    offset = 0
    all_data = []

    batch = [None]  # considered falsy for break at `[]`
    total_records = 0
    while batch:
        paged_url = f"{base_url}?$limit={limit}&$offset={offset}"
        response = requests.get(paged_url)
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        all_data.extend(batch)
        total_records += len(batch)
        logger.info(f"Fetched batch starting at record {offset}, total records fetched: {total_records}")
        offset += limit

    api_data_dataframe = pl.DataFrame(all_data)
    return api_data_dataframe


def convert_dataframe_to_parquet(dataframe):
    buffer = io.BytesIO()
    try:
        dataframe.write_parquet(buffer)
        buffer.seek(0)
        result = buffer
        return result
    except Exception as e:
        logger.error(f"Failed to convert DataFrame to Parquet in-memory: {e}")
        return None


def query_confirmed_planets():
    try:
        tick = time.time()
        logger.info("Querying confirmed exoplanets from NASA Exoplanet Archive")
        all_planets = NasaExoplanetArchive.query_criteria(table="pscomppars", select="*")
        tock = time.time()
        logger.info(f"Query completed in {tock - tick:.2f} seconds")
        pandas_df = all_planets.to_pandas()
        exoplanets_dataframe = pl.from_pandas(pandas_df)
        exoplanets_parquet_buffer = convert_dataframe_to_parquet(exoplanets_dataframe)
        return exoplanets_parquet_buffer
    except Exception as e:
        logger.error(f"Query failed: {e}")


def run_data_ingestion():
    tick = time.time()
    minio_bucket = os.getenv("MINIO_BUCKET_NAME")
    nasa_donki_url = os.getenv("NASA_DONKI_API")
    nasa_apod_url = os.getenv("NASA_APOD_API")
    nasa_donki_filename = "nasa_donki.parquet"
    nasa_apod_filename = "nasa_apod.parquet"

    exoplanets_parquet_buffer = query_confirmed_planets()
    logger.info("Writing Exoplanets Data to MinIO Storage")
    write_data_to_minio(exoplanets_parquet_buffer, minio_bucket, "exoplanets.parquet", "RAW")

    logger.info("Fetching NASA DONKI Data from API")
    nasa_donki_dataframe = fetch_api_data(nasa_donki_url)
    nasa_donki_parquet_buffer = convert_dataframe_to_parquet(nasa_donki_dataframe)
    logger.info("Writing NASA DONKI Data to MinIO Storage")
    write_data_to_minio(nasa_donki_parquet_buffer, minio_bucket, nasa_donki_filename, "RAW")

    logger.info("Fetching NASA APOD Data from API")
    nasa_apod_dataframe = fetch_api_data(nasa_apod_url)
    nasa_apod_parquet_buffer = convert_dataframe_to_parquet(nasa_apod_dataframe)
    logger.info("Writing NASA APOD Data to MinIO Storage")
    write_data_to_minio(nasa_apod_parquet_buffer, minio_bucket, nasa_apod_filename, "RAW")

    tock = time.time() - tick

    logger.info("Synchronizing Data to Database")
    db_sync()

    logger.info(f"Data ingestion completed in {tock:.2f} seconds.")
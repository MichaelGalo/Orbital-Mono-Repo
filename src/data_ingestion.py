import os
import requests
from dotenv import load_dotenv
import io
import polars as pl
import time
from utils import write_data_to_minio
from db_sync import db_sync
from logger import setup_logging
import isodate
from astroquery.ipac.nexsci.nasa_exoplanet_archive import NasaExoplanetArchive
logger = setup_logging()
load_dotenv()

def iso_to_human(iso_str):
    dur = isodate.parse_duration(iso_str)
    total_seconds = int(dur.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    parts = []
    if days:
        parts.append(f"{days} days")
    if hours:
        parts.append(f"{hours} hours")
    if minutes:
        parts.append(f"{minutes} minutes")
    if seconds:
        parts.append(f"{seconds} seconds")
    result = ", ".join(parts) if parts else "0 seconds"
    return result


def fetch_api_data(base_url):
    response = requests.get(base_url)
    response.raise_for_status()
    data = response.json()

    if base_url == os.getenv("THE_SPACE_DEVS_API"):
        astronauts_dataframe = pl.DataFrame(data["results"])

        # flatten select columns
        astronauts_dataframe = astronauts_dataframe.with_columns(
            pl.struct([
                pl.col("agency").struct.field("name").alias("agency_name"),
                pl.col("agency").struct.field("abbrev").alias("agency_abbrev")
            ]).alias("agency_flat"),
            pl.struct([
                pl.col("image").struct.field("image_url").alias("image_url"),
                pl.col("image").struct.field("thumbnail_url").alias("thumbnail_url")
            ]).alias("image_flat"),
        ).unnest(["agency_flat", "image_flat"]).drop(["agency", "image"])

        # parse ISO dates
        astronauts_dataframe = astronauts_dataframe.with_columns(
            pl.col("time_in_space").map_elements(iso_to_human, return_dtype=pl.Utf8).alias("time_in_space_human_readable"),
            pl.col("eva_time").map_elements(iso_to_human, return_dtype=pl.Utf8).alias("eva_time_human_readable")
        )

        result = astronauts_dataframe
        return result

    result = pl.DataFrame(data)
    return result


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


def data_ingestion():
    tick = time.time()

    minio_bucket = os.getenv("MINIO_BUCKET_NAME")

    nasa_donki_url = f"{os.getenv('NASA_DONKI_API')}{os.getenv('NASA_API_KEY')}"
    nasa_apod_url = f"{os.getenv('NASA_APOD_API')}{os.getenv('NASA_API_KEY')}"
    astronaut_url = os.getenv("THE_SPACE_DEVS_API")

    nasa_donki_filename = "nasa_donki.parquet"
    nasa_apod_filename = "nasa_apod.parquet"
    nasa_exoplanets_filename = "nasa_exoplanets.parquet"
    astronaut_filename = "astronauts.parquet"

    exoplanets_parquet_buffer = query_confirmed_planets()
    logger.info("Writing Exoplanets Data to MinIO Storage")
    write_data_to_minio(exoplanets_parquet_buffer, minio_bucket, nasa_exoplanets_filename, "RAW")

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

    logger.info("Fetching Astronaut Data from LL2 API")
    astronaut_dataframe = fetch_api_data(astronaut_url)
    astronaut_parquet_buffer = convert_dataframe_to_parquet(astronaut_dataframe)
    logger.info("Writing Astronaut Data to MinIO Storage")
    write_data_to_minio(astronaut_parquet_buffer, minio_bucket, astronaut_filename, "RAW")

    tock = time.time() - tick

    logger.info("Synchronizing Data to Database")
    db_sync()

    logger.info(f"Data ingestion completed in {tock:.2f} seconds.")
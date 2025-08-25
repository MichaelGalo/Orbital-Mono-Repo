from logger import setup_logging
import os
import sys
import time
import duckdb
from utils import update_data
from dotenv import load_dotenv
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
load_dotenv()

logger = setup_logging()

def db_sync():
    total_start_time = time.time()
    logger.info("Starting Orbital database sync")

    logger.info("Installing and loading DuckDB extensions")
    duckdb.install_extension("ducklake")
    duckdb.install_extension("httpfs")
    duckdb.load_extension("ducklake")
    duckdb.load_extension("httpfs")
    logger.info("DuckDB extensions loaded successfully")

    db_path = os.path.join(parent_path, "orbital.db")
    con = duckdb.connect(db_path)
    logger.info(f"Connected to persistent DuckDB database: {db_path}")

    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")

    logger.info(f"Attaching DuckLake with data path: {data_path}")
    con.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
    con.execute("USE my_ducklake")
    logger.info("DuckLake attached and activated successfully")

    logger.info("Configuring MinIO S3 settings")
    con.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY')}'")
    con.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY')}'")
    con.execute(f"SET s3_endpoint = '{os.getenv('MINIO_EXTERNAL_URL')}'")
    con.execute("SET s3_use_ssl = false")
    con.execute("SET s3_url_style = 'path'")
    logger.info("MinIO S3 configuration completed")

    logger.info("Creating database schemas")
    con.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    con.execute("CREATE SCHEMA IF NOT EXISTS STAGED")
    con.execute("CREATE SCHEMA IF NOT EXISTS CLEANED")
    logger.info("Database schemas created successfully")


    logger.info("Starting data update from what was found in MinIO.")
    minio_bucket = os.getenv('MINIO_BUCKET_NAME')

    # inits db & refreshes on data updates
    update_data(con, logger, minio_bucket, "RAW")

    with open('SQL/staged.sql', 'r') as file:
        staging_query = file.read()
    con.execute(staging_query)

    with open('SQL/cleaned.sql', 'r') as file:
        cleaning_query = file.read()
    con.execute(cleaning_query)

    con.close()
    logger.info("Database connection closed")

    total_end_time = time.time()
    logger.info(f"Database sync completed in {total_end_time - total_start_time:.2f} seconds")
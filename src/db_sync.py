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

def execute_SQL_file(con, file_path):
    full_path = os.path.join(parent_path, file_path)
    if not os.path.exists(full_path):
        logger.error(f"SQL file not found: {full_path}")
        raise FileNotFoundError(full_path)

    with open(full_path, 'r') as file:
        sql = file.read()
    con.execute(sql)

def db_sync():
    total_start_time = time.time()
    logger.info("Starting Orbital database sync")

    logger.info("Installing and loading DuckDB extensions")
    duckdb.install_extension("ducklake")
    duckdb.install_extension("httpfs")
    duckdb.load_extension("ducklake")
    duckdb.load_extension("httpfs")
    logger.info("DuckDB extensions loaded successfully")

    con = duckdb.connect(':memory:')
    logger.info(f"Connected to in-memory DuckDB database")

    data_path = os.path.join(parent_path, "data")
    internal_data = os.path.join(data_path + "/")
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

    # clean up old files (no timeline)
    con.execute("CALL ducklake_expire_snapshots('my_ducklake', older_than => now())")
    con.execute("CALL ducklake_cleanup_old_files('my_ducklake', cleanup_all => true)")

    staged_queries = [
        'SQL/STAGED_ASTRONAUTS.sql',
        'SQL/STAGED_NASA_APOD.sql',
        'SQL/STAGED_NASA_DONKI.sql',
        'SQL/STAGED_NASA_EXOPLANETS.sql'
    ]

    cleaned_queries = [
        'SQL/CLEANED_ASTRONAUTS.sql',
        'SQL/CLEANED_NASA_APOD.sql',
        'SQL/CLEANED_NASA_DONKI.sql',
        'SQL/CLEANED_NASA_EXOPLANETS.sql'
    ]

    for file in staged_queries:
        try:
            logger.info(f"Executing staged query from file: {file}")
            execute_SQL_file(con, file)
        except Exception as e:
            logger.error(f"Error executing staged query from file {file}: {e}")

    for file in cleaned_queries:
        try:
            logger.info(f"Executing cleaned query from file: {file}")
            execute_SQL_file(con, file)
        except Exception as e:
            logger.error(f"Error executing cleaned query from file {file}: {e}")

    con.close()
    logger.info("Database connection closed")

    total_end_time = time.time()
    logger.info(f"Database sync completed in {total_end_time - total_start_time:.2f} seconds")
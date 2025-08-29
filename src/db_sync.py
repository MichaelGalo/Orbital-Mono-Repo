from logger import setup_logging
import os
import sys
import time
from utils import duckdb_con_init, ducklake_init, ducklake_attach_minio, ducklake_refresh, schema_creation, execute_SQL_file, update_data
from data_quality import run_data_quality_checks
from dotenv import load_dotenv
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)
load_dotenv()

logger = setup_logging()

def db_sync():
    total_start_time = time.time()
    logger.info("Starting Orbital database sync")
    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")
    minio_bucket = os.getenv('MINIO_BUCKET_NAME')

    con = duckdb_con_init()
    ducklake_init(con, data_path, catalog_path)
    ducklake_attach_minio(con)
    schema_creation(con)
    update_data(con, logger, minio_bucket, "RAW")
    ducklake_refresh(con)

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

    execute_SQL_file(con, staged_queries)

    if data_path + "/STAGED":
        if run_data_quality_checks() == True:
            execute_SQL_file(con, cleaned_queries)
        else:
            logger.warning("Data quality checks failed. Continuing to use most recent successful data.")
            pass
    
    con.close()
    logger.info("Database connection closed")

    total_end_time = time.time()
    logger.info(f"Database sync completed in {total_end_time - total_start_time:.2f} seconds")
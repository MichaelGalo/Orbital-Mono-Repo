from src.logger import setup_logging
import os
from src.utils import duckdb_con_init, ducklake_init, ducklake_refresh, execute_SQL_file_list, update_data, ducklake_attach_gcp, gcs_path_exists, update_catalog_to_gcs
from src.data_quality import passed_data_quality_checks
from dotenv import load_dotenv
from prefect import task
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))

load_dotenv()

logger = setup_logging()

@task(name="database_sync")
def db_sync():
    logger.info("Starting Orbital database sync")
    gcp_bucket = os.getenv('GCP_BUCKET_NAME')
    data_path = f"gs://{gcp_bucket}/CATALOG_DATA_SNAPSHOTS"
    catalog_path = os.path.join(parent_path, "catalog.ducklake")

    con = duckdb_con_init()
    ducklake_init(con, data_path, catalog_path)
    ducklake_attach_gcp(con)
    update_data(con, logger, gcp_bucket, "RAW_DATA", storage_type="s3")
    ducklake_refresh(con)

    staged_queries = [
        'SQL/staging/STAGED_ASTRONAUTS.sql',
        'SQL/staging/STAGED_NASA_APOD.sql',
        'SQL/staging/STAGED_NASA_DONKI.sql',
        'SQL/staging/STAGED_NASA_EXOPLANETS.sql'
    ]

    cleaned_queries = [
        'SQL/cleaned_aggregation/CLEANED_ASTRONAUTS.sql',
        'SQL/cleaned_aggregation/CLEANED_NASA_APOD.sql',
        'SQL/cleaned_aggregation/CLEANED_NASA_DONKI.sql',
        'SQL/cleaned_aggregation/CLEANED_NASA_EXOPLANETS.sql'
    ]

    execute_SQL_file_list(con, staged_queries)
    ducklake_refresh(con)

    staged_dir = f"gs://{gcp_bucket}/CATALOG_DATA_SNAPSHOTS/STAGED"
    if gcs_path_exists(staged_dir):
        if passed_data_quality_checks():
            execute_SQL_file_list(con, cleaned_queries)
            ducklake_refresh(con)
        else:
            logger.warning("Data quality checks failed. Continuing to use most recent successful data.")
            pass
    
    con.close()
    logger.info("Database connection closed")
    update_catalog_to_gcs(gcp_bucket,catalog_path)
    logger.info("Database sync completed")
import os
from src.logger import setup_logging
from src.utils import duckdb_con_init, ducklake_init, ducklake_attach_gcp, schema_creation, update_catalog_to_gcs
from dotenv import load_dotenv
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))

load_dotenv()
logger = setup_logging()

def setup():
    logger.info("Starting Orbital data lakehouse setup")
    gcp_bucket = os.getenv('GCP_BUCKET_NAME')
    data_path = f"gs://{gcp_bucket}/CATALOG_DATA_SNAPSHOTS"
    catalog_path = os.path.join(parent_path, "catalog.ducklake")
    con = duckdb_con_init()
    ducklake_init(con, data_path, catalog_path)
    ducklake_attach_gcp(con)
    schema_creation(con)
    con.close()
    update_catalog_to_gcs(gcp_bucket,catalog_path)
    logger.info("Setup completed successfully")

if __name__ == "__main__":
    setup()
import os
import sys
from logger import setup_logging
from utils import duckdb_con_init, ducklake_init, ducklake_attach_minio, schema_creation
from dotenv import load_dotenv
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

load_dotenv()
logger = setup_logging()

def setup():
    logger.info("Starting Orbital data lakehouse setup")
    data_path = os.path.join(parent_path, "data")
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    catalog_path = os.path.join(parent_path, "catalog.ducklake")
    con = duckdb_con_init()
    ducklake_init(con, data_path, catalog_path)
    ducklake_attach_minio(con)
    schema_creation(con)
    con.close()
    logger.info("Setup completed successfully")
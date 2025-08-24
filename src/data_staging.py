import os
import time
from logger import setup_logging
from dotenv import load_dotenv
from utils import write_data_to_minio
from db_sync import db_sync

load_dotenv()
logger = setup_logging()

def data_staging():
    tick = time.time()
    logger.info("Starting data staging and transformations.")

    with open('sql/create_views.sql', 'r') as file:
        sql_query = file.read()
    
    
    # once the catalog views are created, write them to minio and tagging them with the "STAGED" directory
    # db_sync will find the staged files in minio and synchronize them to the database

    
    logger.info("Synchronizing Data to Database")
    db_sync()

    tock = time.time()
    logger.info(f"Data staging and transformations completed in {tock - tick:.2f} seconds.")
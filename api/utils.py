from src.utils import duckdb_con_init, ducklake_init, ducklake_attach_gcp
from src.logger import setup_logging
from fastapi import HTTPException
import os
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
logger = setup_logging()

DATASET_CONFIG = {
    1: {
        "table_name": "CLEANED.ASTRONAUTS"
    },
    2: {
        "table_name": "CLEANED.NASA_APOD"
    },
    3: {
        "table_name": "CLEANED.NASA_DONKI"
    },
    4: {
        "table_name": "CLEANED.NASA_EXOPLANETS"
    }
}

def fetch_single_dataset(dataset_id, offset, limit):
    try:
        dataset_id = int(dataset_id)
        offset = int(offset)
        limit = int(limit)
        logger.info(f"Fetching dataset {dataset_id} with offset={offset}, limit={limit}")
        
        if dataset_id not in DATASET_CONFIG:
            raise ValueError(f"Invalid dataset_id: {dataset_id}")
        
        dataset = DATASET_CONFIG[dataset_id]
        logger.info(f"Using dataset: {dataset['table_name']}")

        gcp_bucket = os.getenv('GCP_BUCKET_NAME')
        data_path = f"gs://{gcp_bucket}/CATALOG_DATA_SNAPSHOTS"
        catalog_path = os.path.join(parent_path, "catalog.ducklake")
        
        con = duckdb_con_init()
        ducklake_init(con, data_path, catalog_path)
        ducklake_attach_gcp(con)

        # Use a fully parameterized query
        query = f"""
            SELECT * FROM {dataset['table_name']}
            OFFSET ?
            LIMIT ?
        """
        logger.info(f"Executing parameterized query on table: {dataset['table_name']}")
        result = con.execute(query, [offset, limit]).fetchall()
        columns = [desc[0] for desc in con.description]

        data = [dict(zip(columns, row)) for row in result]

        logger.info(f"Retrieved {len(data)} records")
        return data
        
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except KeyError as ke:
        logger.error(f"KeyError: {ke}")
        raise HTTPException(status_code=404, detail="Dataset not found")
    except Exception as e:
        logger.error(f"Error fetching dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        con.close()


def get_datasets_list():
    result = []
    for dataset_id, config in DATASET_CONFIG.items():
        # Extract the table name and strip the 'CLEANED.' prefix
        stripped_table_name = config["table_name"].split("CLEANED.")[-1]
        
        result.append({
            "id": dataset_id,
            "dataset": stripped_table_name
        })
    return result
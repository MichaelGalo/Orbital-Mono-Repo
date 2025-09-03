from logger import setup_logging
from utils import duckdb_con_init, ducklake_init, ducklake_attach_minio
import os
import sys
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)


logger = setup_logging()

def passed_data_quality_checks():
    logger.info("Starting Data Quality Checks")
    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")

    con = duckdb_con_init()
    ducklake_init(con, data_path, catalog_path)
    ducklake_attach_minio(con)

    data_quality_queries = [
        "SQL/data_quality/QUALITY_ASTRONAUTS.sql",
        "SQL/data_quality/QUALITY_NASA_APOD.sql",
        "SQL/data_quality/QUALITY_NASA_EXOPLANETS.sql",
        "SQL/data_quality/QUALITY_NASA_DONKI.sql"
    ]

    all_pass = True
    details = {} # used only for debug details

    for rel_path in data_quality_queries:
        full_path = os.path.join(parent_path, rel_path)
        logger.info(f"Running data quality SQL for: {rel_path}")
        with open(full_path, "r") as file:
            data_quality_query = file.read()

        try:
            response = con.execute(data_quality_query)
            # fetchall will return [] for non-selects or selects with 0 rows
            rows = response.fetchall()
            if rows:
                logger.error(f"Data quality FAILED for {rel_path}: {len(rows)} row(s) returned")
                details[rel_path] = {"passed": False, "rows": rows}
                all_pass = False
            else:
                logger.info(f"Data quality passed for {rel_path}")
                details[rel_path] = {"passed": True, "rows": []}
        except Exception as e:
            logger.error(f"Error executing {rel_path}: {e}")
            con.close()

    try:
        con.close()
    except Exception as e:
        logger.warning(f"Failed to close DuckDB connection: {e}")

    return all_pass
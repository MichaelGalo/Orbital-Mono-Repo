from minio import Minio
from logger import setup_logging
import sys
import os
import io
import isodate
import duckdb
import polars as pl
from urllib.parse import urlencode
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

logger = setup_logging()

def execute_SQL_file_list(con, list_of_file_paths):
    for file_path in list_of_file_paths:
        full_path = os.path.join(parent_path, file_path)
        if not os.path.exists(full_path):
            logger.error(f"SQL file not found: {full_path}")
            raise FileNotFoundError(full_path)

        logger.info(f"Executing SQL file: {full_path}")
        with open(full_path, 'r') as file:
            sql = file.read()
        try:
            con.execute(sql)
        except Exception as e:
            logger.error(f"Failed executing {full_path}: {e}")
            raise

def duckdb_con_init():
    logger.info("Installing and loading DuckDB extensions")
    duckdb.install_extension("ducklake")
    duckdb.install_extension("httpfs")
    duckdb.load_extension("ducklake")
    duckdb.load_extension("httpfs")
    logger.info("DuckDB extensions loaded successfully")

    con = duckdb.connect(':memory:')
    logger.info("Connected to in-memory DuckDB database")
    return con

def ducklake_init(con, data_path, catalog_path):
    logger.info(f"Attaching DuckLake with data path: {data_path}")
    con.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
    con.execute("USE my_ducklake")
    logger.info("DuckLake attached and activated successfully")

def ducklake_attach_minio(con):
    logger.info("Configuring MinIO S3 settings")
    con.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY')}'")
    con.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY')}'")
    con.execute(f"SET s3_endpoint = '{os.getenv('MINIO_EXTERNAL_URL')}'")
    con.execute("SET s3_use_ssl = false")
    con.execute("SET s3_url_style = 'path'")
    logger.info("MinIO S3 configuration completed")

def schema_creation(con):
    logger.info("Creating database schemas")
    con.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    con.execute("CREATE SCHEMA IF NOT EXISTS STAGED")
    con.execute("CREATE SCHEMA IF NOT EXISTS CLEANED")
    logger.info("Database schemas created successfully")

def ducklake_refresh(con): # ensures most up to date .parquet is used
    logger.info("Refreshing DuckLake metadata to most up-to-date")
    con.execute("CALL ducklake_expire_snapshots('my_ducklake', older_than => now())")
    con.execute("CALL ducklake_cleanup_old_files('my_ducklake', cleanup_all => true)")

def update_data(con, logger, bucket_name, folder_path): # inits db & refreshes on data updates
    logger.info("Refreshing database with the most current data")
    file_list_query = f"SELECT * FROM glob('s3://{bucket_name}/{folder_path}/*.parquet')"

    try:
        files_result = con.execute(file_list_query).fetchall()
        file_paths = []
        for row in files_result:
            file_paths.append(row[0])
        
        logger.info(f"Found {len(file_paths)} files in MinIO bucket")
        
        for file_path in file_paths:
            file_name = os.path.basename(file_path).replace('.parquet', '')
            table_name = file_name.upper().replace('-', '_').replace(' ', '_')

            logger.info(f"Processing file: {file_path} -> table: {folder_path}.{table_name}")

            query = f"""
            CREATE OR REPLACE TABLE {folder_path}.{table_name} AS
            SELECT 
                *,
                '{file_name}' AS _source_file,
                CURRENT_TIMESTAMP AS _ingestion_timestamp,
                ROW_NUMBER() OVER () AS _record_id
            FROM read_parquet('{file_path}');
            """
            
            con.execute(query)
            logger.info(f"Successfully created or updated {folder_path}.{table_name}")

    except Exception as e:
        logger.error(f"Error processing files from MinIO: {e}")
        raise


def write_data_to_minio(parquet_buffer, bucket_name, object_name, folder_name=None):
    minio_client = Minio(
        os.getenv("MINIO_EXTERNAL_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    parquet_buffer.seek(0)
    data_bytes = parquet_buffer.read()

    if folder_name:
        folder_name = folder_name.strip("/")
        full_object_name = f"{folder_name}/{object_name}"
    else:
        full_object_name = object_name

    try:
        minio_client.put_object(
            bucket_name,
            full_object_name,
            io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/x-parquet",
        )
        logger.info(f"Successfully wrote {full_object_name} to bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Failed to write data to MinIO: {e}")

def add_query_params(base_url, params):
    """
    Append params to base_url without parsing the URL first.
    - Skips params with value None.
    - Values are converted to strings and URL-encoded.
    - Handles whether base_url already contains '?' or ends with '?' or '&'.
    """
    if not params:
        return base_url

    cleaned_params = {}
    for name, value in params.items():
        if value is None:
            continue
        cleaned_params[str(name)] = str(value)

    if not cleaned_params:
        return base_url

    encoded_query = urlencode(cleaned_params, doseq=True)

    if base_url.endswith('?') or base_url.endswith('&'):
        separator = ''
    elif '?' in base_url:
        separator = '&'
    else:
        separator = '?'

    result = f"{base_url}{separator}{encoded_query}"
    return result

def iso_to_human(iso_str):
    duration = isodate.parse_duration(iso_str)
    total_seconds = int(duration.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []

    match (days, hours, minutes, seconds):
        case (days_value, hours_value, minutes_value, seconds_value) if days_value > 0:
            parts.append(f"{days_value} days")
        case (days_value, hours_value, minutes_value, seconds_value) if hours_value > 0:
            parts.append(f"{hours_value} hours")
        case (days_value, hours_value, minutes_value, seconds_value) if minutes_value > 0:
            parts.append(f"{minutes_value} minutes")
        case (days_value, hours_value, minutes_value, seconds_value) if seconds_value > 0:
            parts.append(f"{seconds_value} seconds")
        case _:
            parts.append("0 seconds")

    result =  ", ".join(parts)
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
    
def preprocess_astronaut_data(astronauts_dataframe):
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

def handle_date_adjustment(from_date, years):
    try:
        result = from_date.replace(year=from_date.year - years)
        return result
    except ValueError:
        return from_date.replace(month=2, day=28, year=from_date.year - years)
    

def preprocess_apod_data(apod_dataframe):

    expected_columns = {
        "resource": pl.Utf8,
        "concept_tags": pl.Boolean,
        "title": pl.Utf8,
        "date": pl.Date,
        "url": pl.Utf8,
        "hdurl": pl.Utf8,
        "media_type": pl.Utf8,
        "explanation": pl.Utf8,
        "concepts": pl.Utf8,
        "thumbnail_url": pl.Utf8,
        "service_version": pl.Utf8,
        "copyright": pl.Utf8
    }

    for col, dtype in expected_columns.items():
        if col not in apod_dataframe.columns:
            apod_dataframe = apod_dataframe.with_columns(pl.lit(None).cast(dtype).alias(col))
        else:
            apod_dataframe = apod_dataframe.with_columns(pl.col(col).cast(dtype))
    
    result = apod_dataframe.select(list(expected_columns.keys()))
    return result
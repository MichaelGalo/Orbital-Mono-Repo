import os
import io
from minio import Minio
from logger import setup_logging

logger = setup_logging()


def update_data(con, logger, bucket_name, folder_path):
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


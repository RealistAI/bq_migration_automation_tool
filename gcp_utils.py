from config.config import bq_migration_config
import datetime
from google.cloud import bigquery
from google.cloud import storage
import utils
import os
import re

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def submit_query(query:str,
                 dry_run:bool) -> bigquery.QueryJob:
    """ Submit a query job to BigQuery.

    Args:
    query: A string containing a BigQuery compatible SQL query
    client: A BigQuery client instance
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(dry_run=dry_run)
    try:
        logger.info(f"Submitting query to BigQuery:\n{query}\n")
        query_job = client.query(query=query,
                                 job_config=job_config)
        return query_job
    except Exception as error:
        return error


def validate_sql(sql_to_validate):
    logger.info(f'Validating {sql_to_validate}')
    with open (sql_to_validate, 'r') as file:
        data = file.read()

    logger.debug(f'Submitting {sql_to_validate} for dry-run')
    try:
        query_job = submit_query(query=data,
                                 dry_run=True)
        logger.info(f'{sql_to_validate} was valid')
        return True
    except Exception as error:
        data = [file_name,error]
        utils.append_to_csv_file(csv_file_path='invalid_sql/failure_logs.csv',
                                 data=data)
        logger.info(f'{sql_to_validate} was invalid')
        return False


def create_bucket(bucket_name:str,
                  storage_class:str,
                  location:str) -> None:
    """
    Creates bucket in GCS.

    Args:
    bucket_name: Name of bucket to be created.
    """
    client = storage.Client()
    try:
        bucket = client.bucket(bucket_name)
        bucket.storage_class = storage_class
    except Exception as error:
        logger.exception(f'Bucket configuration invalid: {error}')
    try:
        new_bucket = client.create_bucket(bucket,
                                          location=location)
    except Exception as error:
        logger.exception(f'Failed to create bucket with error: {error}')

    logger.info(f'Successfully created bucket: gs://{bucket_name}')

if __name__ == "__main__":
    source = f'{bq_migration_config["transpiled_sql"]}/{bq_migration_config["file_name"]}'
    validate_sql(sql_to_validate=source,
                 output_validated_sql=bq_migration_config['validated_sql_output'])

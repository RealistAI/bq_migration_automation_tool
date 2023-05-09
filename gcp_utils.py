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
        logger.info("Submitting query to BigQuery:\n{query}\n")
        query_job = client.query(query=query,
                                 job_config=job_config)
        return query_job
    except Exception as error:
        logger.exception(f'Query failed with error: {error}')


def validate_sql(sql_to_validate,
                 output_validated_sql,
                 output_invalid_sql):
    invalid_sql = []
    validated_sql = []
    regex_pattern = r'SELECT[\s\S]*?;'
    current_datetime = datetime.datetime.now()
    invalid_sql_file = f'{output_invalid_sql}/batch_{current_datetime}.txt'
    validated_sql_file = f'{output_validated_sql}/b1atch_{current_datetime}.txt'

    if os.path.isfile(sql_to_validate):
        with open (sql_to_validate, 'r') as file:
            data = file.read()
        logger.debug(data)


        sql_queries = re.findall(regex_pattern, data)
        for query in sql_queries:
            logger.debug(f'Submitting query: {query}')
            query_job = submit_query(query=query,
                                     dry_run=True)

            if query_job is None:
                invalid_sql = invalid_sql.append(query)
                utils.create_file(file_name=invalid_sql_file)
            elif query_job.totalBytesProcessed != None:
                validate_sql = validate_sql.append(query)
                utils.create_file(file_name=validated_sql_file)

    utils.write_list_to_file(file=invalid_sql_file,
                             list_to_write=invalid_sql)
    utils.write_list_to_file(file=validated_sql_file,
                             list_to_write=validated_sql)


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
    validate_sql(sql_to_validate=bq_migration_config['output'],
                 output_validated_sql=bq_migration_config['validated_sql_output'],
                 output_invalid_sql=bq_migration_config['invalid_sql_output'])

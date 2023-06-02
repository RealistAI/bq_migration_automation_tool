import datetime
import config
from google.cloud import bigquery
from google.cloud import storage
import os
import transpilation_logs as tl

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
        query_results = client.query(query=query,
                                 job_config=job_config).result()
        return query_results
    except Exception as error:
        return error

def submit_query_for_validation(query:str,
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
        query_results = client.query(query=query,
                                 job_config=job_config)
        return query_results
    except Exception as error:
        return error


def validate_sql(sql_to_validate,
                 uc4_job_name) -> bool:
    """
    Validates the .sql files that are being brought in. All logs, successful or not, are uploaded to the transpilation_logs table in BigQuery.

    Args:
    sql_to_validate: the path to the sql to validate.
    uc4_job_name: the name of the sql job being validated.
    """

    logger.info(f'Validating {sql_to_validate}')
    with open (sql_to_validate, 'r') as file:
        data = file.read()
    print("data is ", type(data), data)

    logger.debug(f'Submitting {sql_to_validate} for dry-run')
    query_job = submit_query_for_validation(query=data,
                                            dry_run=True)

    current_datetime = str(datetime.datetime.now())
    if isinstance(query_job, bigquery.QueryJob):
        logger.info("validation successful")
        tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id=config.DATASET, job_id=uc4_job_name, status="SUCCEEDED", message="null", query=data, run_time=current_datetime)
        return True

    elif isinstance(query_job, Exception):
        logger.info("validation failed")
        tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id=config.DATASET, job_id=uc4_job_name, status="FAILED", message=query_job, query=data, run_time=current_datetime)
        return False

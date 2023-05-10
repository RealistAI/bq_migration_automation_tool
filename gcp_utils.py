import datetime
import config
from google.cloud import bigquery
from google.cloud import storage
import utils
import os

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


def validate_sql(sql_to_validate,
                 file_name):
    logger.info(f'Validating {sql_to_validate}')
    with open (sql_to_validate, 'r') as file:
        data = file.read()

    logger.debug(f'Submitting {sql_to_validate} for dry-run')
    query_job = submit_query(query=data,
                             dry_run=True)

    if isinstance(query_job, bigquery.QueryJob):
        return True
    elif isinstance(query_job, Exception):
        data = [file_name,query_job]
        current_datetime = str(datetime.datetime.now())
        stripped_datetime = utils.remove_non_alphanumeric(string=current_datetime)
        csv_file_path = f'{config.FAILURE_LOGS}/{stripped_datetime}.csv'
        utils.append_to_csv_file(csv_file_path=csv_file_path,
                                 data=data)
        return False

from google.cloud import bigquery
import config
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def transpile_logs_into_table(project_id,
                              dataset_id,
                              job_id,
                              status,
                              message,
                              query,
                              run_time) -> None:
    """
    Takes the dry-run logs and puts them into the transpilation_logs table in BQ.
    Args:
    project_id: the project being used to access the transpilation_logs table.
    dataset_id: the dataset being used to access the transpilation_logs table.
    job_id: the name of the uc4 job being put into BigQuery.
    status: The status of the transpilation. SUCCEEDED|FAILED.
    query: the specific query that failed in the validation process.
    message: the error message.
    run_time: when the transpilation job ran.
    """
    client= bigquery.Client()
    try:
        insert_changes_query = client.query(f"""
                                            INSERT INTO {project_id}.{dataset_id}.transpilation_logs (job_id, status, message, query, run_time)
                                            VALUES ('''{job_id}''', '''{status}''', '''{message}''', '''{query}''', '''{run_time}''')
                                            """)

        results = insert_changes_query.result()
        logger.info(f"{results} uploaded to transpilation_logs table")
        return results

    except Exception as error:
        logger.info(query)
        logger.info(error)

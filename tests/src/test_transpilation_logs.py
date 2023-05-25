from google.cloud import bigquery
import config
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestTranspilationLogs:
    def test_create_log_table_successfully(self):
        pass

    def test_create_log_table_failed_due_to_invalid_project_id(self):
        pass

    def test_transpile_logs_into_table_successfully(self):
        pass

    def test_transpile_logs_into_table_failed_due_to_invalid_dataset_id(self):
        pass

def create_transpilation_log_table(project_id,
                                   dataset_id) -> None:
    """
    creates a table with the transpilation logs of the sqls being dry run. it shows the name of the job, the status of either success or fail and the when the dry run began.

    Args:
    project_id: the project being used to create the transpilation_logs table.
    dataset_id: the dataset being used to create the transpilation_logs table.
    """
    client = bigquery.Client()
    try:
        create_table_query = client.query(f"""
                                          CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.transpilation_logs(
                                              job_id STRING,
                                              status STRING,
                                              message STRING,
                                              run_time TIMESTAMP
                                          );""")

        results = create_table_query.result()

        for row in results:
            logger.info(f"{row.url} : {row.view_count}")

    except Exception as error:
        logger.info(error)

#create_transpilation_log_table(config.PROJECT, config.DATASET)

def transpile_logs_into_table(project_id,
                              dataset_id,
                              job_id,
                              status,
                              message,
                              run_time) -> None:
    """
    Takes the dry-run logs and puts them into the transpilation_logs table in BQ.

    Args:
    project_id: the project being used to access the transpilation_logs table.
    dataset_id: the dataset being used to access the transpilation_logs table.
    job_id: the name of the uc4 job being put into BigQuery.
    status: The status of the transpilation. SUCCEEDED|FAILED.
    message: the error message.
    run_time: when the transpilation job ran.
    """
    client= biquery.Client()
    try:
        insert_changes_query = client.query(f"""
                                            INSERT INTO {project_id}.{dataset_id}.transpilation_log (job, status, message, run_time)
                                            VALUES ('{job}', '{status}', '{message}', '{run_time}')
                                            """)

        results = insert_changes_query.result()
        logger.info(results)

    except Exception as error:
        logger.info(error)

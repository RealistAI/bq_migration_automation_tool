from google.cloud import bigquery
import config
import logging
import transpilation_logs as tl
import datetime
import pytest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestTranspilationLogs:
    def test_create_log_table_successfully(self):
        create_table = tl.create_transpilation_log_table(project_id=config.PROJECT, dataset_id="merriks_dataset")
        assert create_table == None

    def test_create_log_table_failed_due_to_invalid_project_id(self):
        with pytest.raises(Exception):
            tl.create_transpilation_log_table(project_id="not a real project", dataset_id=config.DATASET)

    def test_transpile_logs_into_table_with_success_data(self):
        current_datetime = str(datetime.datetime.now())
        insert_values =  tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id="merriks_dataset", job_id="uc4_test_job_1", status="SUCCEEDED", message="null", run_time=current_datetime)
        assert insert_values != Exception

    def test_transpile_logs_into_table_with_fail_data(self):
        current_datetime = str(datetime.datetime.now())
        insert_values =  tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id="merriks_dataset", job_id="uc4_test_job_2", status="FAILED", message="Expected keyword FROM but got NOT at [5:21]", run_time=current_datetime)
        assert insert_values != Exception


    def test_transpile_logs_into_table_failed_due_to_nonexistent_dataset_id(self):
        with pytest.raises(Exception):
            tl.transpile_logs_into_table(project_id=config.PROJECT, dataset_id="not a real datatset", job_id="uc4_test_job_1", status="SUCCEEDED", message="null", run_time=current_datetime)

@pytest.fixture(scope="session")
def delete_table():
    client = bigquery.Client()
    yield
    try:
        delete_table = client.query(f"""
                                    DROP TABLE {config.PROJECT}.{"merriks_dataset"}.transpilation_logs;
                                    """
                                    )
        results = delete_table.result()
        logger.info(results)
    except Exception as error:
        logger.info(f"Error is {error}")

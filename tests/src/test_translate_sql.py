import config
import os
import pytest
import json
import translate_sql as ts
from google.cloud import bigquery
import shutil
import utils
from pathlib import Path
from google.api_core.exceptions import NotFound, Forbidden, BadRequest, ServiceUnavailable, Conflict, TooManyRequests

class TestTranslateSQL:
    def test_split_table_reference_success(self):
        table_reference = "test_dataset.test_table"
        split_table = ts.split_table_reference(table_reference)
        assert type(split_table) == type(tuple())
        assert split_table == ("test_dataset", "test_table")

    def test_split_table_reference_fail_due_malformed_reference(self):
        with pytest.raises(AssertionError):
            table_reference = "Hello there"
            ts.split_table_reference(table_reference)

    def test_generate_object_mapping_success(self,
                                             setup):
        client = setup
        generate_mapping = ts.generate_object_mapping(client=client)
        assert generate_mapping == None

    def test_generate_object_mapping_fal_due_to_bad_client(self):
        client = "client"
        with pytest.raises(AttributeError):
            ts.generate_object_mapping(client=client)

    def test_validate_sqls_success(self,
                                   setup,
                                   create_and_delete_file):
        client = setup
        uc4_jobs = ["UC4_JOB_1", "UC4_JOB_2"]
        uc4_sql_dependencies =  {"UC4_JOB_1": ["test_file.sql"], "UC4_JOB_2": ["test_file2.sql"]}
        validate_sql = validate_sqls_test(client=client,
                                          uc4_jobs=uc4_jobs,
                                          uc4_sql_dependencies=uc4_sql_dependencies)
        assert validate_sql == None


    def test_validate_sqls_fail_due_to_invalid_jobs(self,
                                setup):
        client = setup
        uc4_jobs = ["batman"]
        uc4_sql_dependencies = {"sql_paths": "output/bteq/BU/SIMBA/AMPS/"}
        with pytest.raises(KeyError):
            validate_sqls_test(client=client,
                             uc4_jobs=uc4_jobs,
                             uc4_sql_dependencies=uc4_sql_dependencies)


    def test_write_logs_to_table_success(self,
                                         setup,
                                         delete_table):
        client = setup
        uc4_job = "UC4_JOB_1"
        result = "SUCEEDED"
        message = "null"
        dry_run_sql = f"SELECT * FROM mytable WHERE job = {uc4_job}"
        referenced_sqls = "output/bteq/BU/SIMBA/AMPS/sql_file2.sql"
        write_logs = write_log_to_table_test(client=client,
                                           uc4_job=uc4_job,
                                           result=result,
                                           message=message,
                                           dry_run_sql=dry_run_sql,
                                           referenced_sqls=referenced_sqls)
        assert write_logs == None


@pytest.fixture(scope="session")
def create_and_delete_file():
    os.system("""
              cd ~/bqms/output/;
              touch test_file.sql;
              touch test_file2.sql;
              echo "SELECT * FROM my_table" > test_file.sql;
              echo "SELECT * FROM my_table2" > test_file2.sql;
              """)
    yield
    os.system("""
              cd ~/bqms/output/;
              rm test_file.sql;
              rm test_file2.sql;
              """)


@pytest.fixture(scope="session")
def delete_table(setup):
    client = setup
    yield
    client.query("DROP TABLE michael-gilbert-dev.UC4_Jobs.test_logs_table")

@pytest.fixture(scope="session")
def setup():
    """
    This method ensures that all of the items required by this script are
    created and available.
    """

    # Make sure the BQMS Folder exists and is empty
    if os.path.exists(config.BQMS_FOLDER):
        shutil.rmtree(config.BQMS_FOLDER)

    for path in [
            config.BQMS_FOLDER,
            config.BQMS_INPUT_FOLDER,
            config.BQMS_OUTPUT_FOLDER,
            config.BQMS_CONFIG_FOLDER
            ]:
        utils.create_path_if_not_exists(path)

    # Download the repo containing all of the SQLs
    utils.create_path_if_not_exists(config.BASE_PATH)
    utils.get_git_repo(repo=config.UC4_SQL_REPO,
                     base_path=config.BASE_PATH)
    # Make sure the UC4 Config file exists
    assert config.UC4_CSV_FILE.is_file(), "The uc4_jobs.csv file is expected "\
            f"to be available here '{config.UC4_CSV_FILE}' but it does not"\
            " exist. Please refer to the README.md for instructions on how to"\
            " create it."

    # Make sure the Business Unit Dataset Map file exists
    assert config.BUSINESS_UNIT_DATASET_MAP_CSV_FILE.is_file(), \
            "The business_unit_dataset_map.csv file is expected te be "\
            f"available here '{config.BUSINESS_UNIT_DATASET_MAP_CSV_FILE}' "\
            "but it does not exist. Please refer to the README.md for "\
            "instructions on how to create it."

    client = bigquery.Client(project=config.METADATA_PROJECT)

    # Create the teradata_to_bigquery_map table if it doesn't exist
    query = f"CREATE TABLE IF NOT EXISTS michael-gilbert-dev.UC4_Jobs.test_logs_table (\n"\
            "  uc4_job  STRING,\n"\
            "  status STRING,\n"\
            "  message STRING,\n"\
            "  dry_run_sql STRING,\n"\
            "  referenced_sql_files STRING,\n"\
            "  timestamp TIMESTAMP\n"\
            ")"

    utils.submit_query(client=client, query=query)

    return client


def validate_sqls_test(client,
                       uc4_jobs,
                       uc4_sql_dependencies):
    """
    We need to validate the SQLs that have been translated. It is important
    that we submit all of the SQLs for a given UC4 job at the same time as
    there may be cases where one SQL creates a table/view that is read by
    another.

    We are going to collect all of the SQL code for the UC4 job provided and
    submit it to BigQuery as a single statement.

    We will then write the dry-run result to BigQuery
    """

    # Iterate through the UC4 Jobs and read the SQL files from the output 
    # folder into a list
    for uc4_job in uc4_jobs:
        if uc4_job == "":
            continue

        sqls = []
        for sql_ref in uc4_sql_dependencies[uc4_job]:
            # Create a path to the output folder where the SQL should reside
            sql_path = Path(config.BQMS_OUTPUT_FOLDER, sql_ref)

            print(f"Collecting '{sql_path}' for dry-run")
            assert sql_path.exists, f"SQL Path referenced by {uc4_job} " \
                    "does not exist: {sql_path}"

            with open(sql_path, 'r') as sql_file:
                sqls.append(sql_file.read())

        # Dry run the SQLs
        query = '\n'.join(sqls)
        result, message  = utils.submit_dry_run(client=client,
                                                query=query)

        if result == 'SUCCEEEDED':
            print(f"dry-run for {uc4_job} succeeded")
        else:
            print(f"dry-run for {uc4_job} failed.")

        sql_references = []
        for ref in uc4_sql_dependencies[uc4_job]:
            sql_references.append(str(ref))

        write_log_to_table_test(client=client, uc4_job=uc4_job, result=result,
                                   message=message, dry_run_sql=query,
                                   referenced_sqls='\n'.join(sql_references))

    os.system(f"cp -r {config.BQMS_OUTPUT_FOLDER} {config.TARGET_SQL_PATH}")


def write_log_to_table_test(client, uc4_job, result,
                       message, dry_run_sql, referenced_sqls):
    """
    Log the results of the dry-run to the BigQuery log table.
    """

    # assuming we only need the first line of the 
    query = f"INSERT INTO michael-gilbert-dev.UC4_Jobs.test_logs_table " \
            "(uc4_job, status, message, dry_run_sql, referenced_sql_files, " \
            "timestamp)\n" \
            f"VALUES('{uc4_job}','{result}', \"\"\"{message}\"\"\", " \
            f"\"\"\"{dry_run_sql}\"\"\", \"\"\"{referenced_sqls}\"\"\", " \
            "CURRENT_TIMESTAMP());"

    print(query)
    utils.submit_query(client=client, query=query)

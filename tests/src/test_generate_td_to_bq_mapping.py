import pytest
import config
from google.cloud import bigquery
import generate_td_to_bq_mapping as gm
import os
from  pathlib import Path
import utils
from google.api_core.exceptions import NotFound, Forbidden, BadRequest, ServiceUnavailable, Conflict, TooManyRequests

class TestObjectMapping:

    def test_get_created_tables_and_views_success(self,
                                                  create_sql_file):
        file_path = "test_file.sql"
        sql_path = Path(file_path)
        get_tables_and_views = gm.get_created_tables_and_views(sql_path)
        assert type(get_tables_and_views) == type(list())

    def test_get_created_tables_and_views_fail_due_to_invalid_path(self):
        with pytest.raises(AttributeError):
            gm.get_created_tables_and_views(sql_path="file.fake")

    def test_write_table_mapping_to_bq_success(self,
                                               bq_client,
                                               create_and_delete_table):
        client = bq_client
        table_map = {"key1": "item1", "key2": "item2", "key3": "item3"}
        table_mappings = write_table_mapping_to_bigquery_test(client=client,
                                                              table_map=table_map)
        print(table_mappings, type(table_mappings))
        assert table_mappings == None

    def test_write_table_mapping_to_bq_fail_due_to_using_list(self,
                                                              bq_client):
        with pytest.raises(AttributeError):
            client = bq_client
            table_map = ["key1", "item1", "key2", "item2", "key3", "item3"]
            write_table_mapping_to_bigquery_test(client=client,
                                                 table_map=table_map)

    def test_map_table_references_success(self,
                                          bq_client):
        client = bq_client
        business_unit = "CREDIT"
        table_references = ["table1.item1", "table2.item2", "table3.item", "table4.item4", "table5.item5"]
        map_references = gm.map_table_references(client=client,
                                                 table_references=table_references,
                                                 business_unit=business_unit)
        assert map_references == None

    def test_map_table_references_fail_due_to_using_dict(self,
                                                         bq_client):
        client = bq_client
        business_unit = "CREDIT"
        table_references = {"table1": "item1", "table2": "item2", "table3": "item3", "table4": "item4", "table5": "item5"}
        with pytest.raises(AssertionError):
            gm.map_table_references(client=client,
                                    table_references=table_references,
                                    business_unit=business_unit)

@pytest.fixture(scope="session")
def setup():
    """
    This method ensures that all of the items required by this script are
    created and available.
    """
    # Download the repo containing all of the SQLs
    utils.create_path_if_not_exists(config.BASE_PATH)
    utils.get_git_repo(repo=config.UC4_SQL_REPO,
                     base_path=config.BASE_PATH)
    # Make sure the UC4 Config file exists
    assert config.UC4_CSV_FILE.is_file(), "The uc4_jobs.csv file is expected "\
            "to be available here '{config.UC4_CSV_FILE}' but it does not"\
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
    query = f"CREATE TABLE IF NOT EXISTS {config.TD_TO_BQ_MAPPING_TABLE} (\n"\
            "  teradata_table STRING,\n"\
            "  bigquery_table STRING"\
            ")"

    utils.submit_query(client=client, query=query)

    return client


@pytest.fixture(scope="session")
def bq_client():
    client = bigquery.Client()
    return client


@pytest.fixture(scope="session")
def create_and_delete_table():
    client = bigquery.Client()

    create_table_query = client.query("""
            CREATE TABLE michael-gilbert-dev.UC4_Jobs.test_mapping_table(
                teradata_table STRING,
                bigquery_table STRING
            );""")
    results = create_table_query.result()

    for row in results:
        print(f"{row.url} : {row.view_count}")
    yield
    client.query("DROP TABLE michael-gilbert-dev.UC4_Jobs.test_mapping_table")


@pytest.fixture(scope="session")
def create_sql_file():
    os.system("""
              cd ~/git/bq_migration_automation_tool;
              touch test_file.sql;
              echo "CREATE TABLE my_dataset.data_table" > test_file.sql;
              """)

def write_table_mapping_to_bigquery_test(client,
                                         table_map):
    """
    Parse the table map dictionary and write the mappings to BigQuery
    """

    print("Inserting the Teradata to BigQuery mappings into the test_mapping_table table")
    query = []
    source_table_list = list(table_map.keys())
    parameters = ',\n'.join("'" + item + "'" for item in source_table_list)
    query.append(
            f"DELETE FROM  michael-gilbert-dev.UC4_Jobs.test_mapping_table\nWHERE "\
            f"teradata_table in ({parameters})")

    values_list = []
    for key, value in table_map.items():
        # The transpiler will always convert the table and dataset names 
        # to lowercase. We must do the same in our mapping to ensure the 
        # transpiler recognizes the table names
        values_list.append(f"('{key.lower()}', '{value.lower()}')")

    query.append(f"INSERT INTO  michael-gilbert-dev.UC4_Jobs.test_mapping_table ("\
            "teradata_table, bigquery_table) \n" \
            f"VALUES{','.join(values_list)}")

    utils.submit_query(client=client, query=';\n'.join(query))
    print("  Successfully wrote mappings to test_mapping_table")

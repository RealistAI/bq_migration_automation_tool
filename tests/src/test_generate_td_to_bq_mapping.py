import pytest
import config
from google.cloud import bigquery
import generate_td_to_bq_mapping as gm
import os
from  pathlib import Path

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

    def test_write_table_mapping_to_bq_success(self, bq_client):
        client = bq_client
        table_map = {"key1": "item1", "key2": "item2", "key3": "item3"}
        table_mappings = gm.write_table_mapping_to_bigquery(client=client,
                                                            table_map=table_map)
        print(table_mappings, type(table_mappings))
        assert table_mappings == None

    def test_write_table_mapping_to_bq_fail_due_to_using_list(self, bq_client):
        with pytest.raises(AttributeError):
            client = bq_client
            table_map = ["key1", "item1", "key2", "item2", "key3", "item3"]
            gm.write_table_mapping_to_bigquery(client=client,
                                               table_map=table_map)

    def test_map_table_references_success(self, bq_client):
        client = bq_client
        business_unit = "CREDIT"
        table_references = ["table1 item1", "table2 item2", "table3 item3", "table4 item4", "table5 item5"]
        map_references = gm.map_table_references(client=client,
                                                 table_references=table_references,
                                                 business_unit=business_unit)
        assert map_references == None

    def test_map_table_references_fail_due_to_using_dict(self, bq_client):
        client = bq_client
        business_unit = "CREDIT"
        table_references = {"table1": "item1", "table2": "item2", "table3": "item3", "table4": "item4", "table5": "item5"}
        with pytest.raises(AssertionError):
            gm.map_table_references(client=client,
                                    table_references=table_references,
                                    business_unit=business_unit)


    #def test_main_success(self):
    #    pass

    #def test_main_fail(self):
    #    pass

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
def create_sql_file():
    os.system("""
              cd ~/git/bq_migration_automation_tool;
              touch test_file.sql;
              echo "CREATE TABLE my_dataset.data_table" > test_file.sql;
              """)

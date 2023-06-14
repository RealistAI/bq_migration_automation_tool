import pytest
import config
from google.cloud import bigquery

class TestObjectMapping:

    def test_get_created_tables_and_views_success(self):
        pass

    def test_get_created_tables_and_views_fail(self):
        pass

    def get_business_unit_map_success(self):
        pass

    def get_business_unit_map_fail(self):
        pass

    def write_table_mapping_to_bq_success(self):
        pass

    def write_table_mapping_to_bq_fail(self):
        pass

    def test_map_table_references_success(self):
        pass

    def test_map_table_references_fail(self):
        pass

    def test_main_success(self):
        pass

    def test_main_fail(self):
        pass

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

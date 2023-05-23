from utils import gcp
import pytest
import sort_queries as s
import config
import os

class TestSortQueries:
    def test_sort_queries_successfully(self, create_directories):
        sort_queries = s.sort_queries(project=config.PROJECT, dataset=config.DATASET)
        os.system("rm -r output/")
        assert sort_queries == None

    def test_sort_queries_fail_due_to_non_existent_project(self):
        with pytest.raises(Exception):
            s.sort_queries(project="not a real project", dataset=config.DATASET)

@pytest.fixture(scope="session")
def create_directories():
    os.system("""
              mkdir output;
              cd output;
              mkdir bteq;
              cd bteq;
              mkdir BU;
              cd BU;
              mkdir SIMBA;
              cd SIMBA;
              mkdir AMPS;
              cd AMPS;
              touch sql_1.sql;
              touch sql_2.sql;
              """)


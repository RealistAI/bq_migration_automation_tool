import pytest
from google.cloud import bigquery
from google.cloud import storage
import logging
from bq_migration_automation_tool import gcp_utils as gcp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestGCP:
    def test_validate_sql_successfully(self):
         validate_sql = gcp.submit_query(sql_to_validate="",
                                         file_name="")


    def test_validate_sql_fail_due_to_invalid_sql(self):
        pass

#    def test_submit_query_successfully(self):
#        submut_query = gcp.submit_query(query="""SELECT * FROM MyTable LIMIT 10""",
                                        dry_run =

#    def test_submit_query_failed_due_to_invalid_query(self):
#        pass


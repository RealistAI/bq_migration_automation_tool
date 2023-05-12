import pytest
from google.cloud import bigquery
from google.cloud import storage
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestGCP:
    def test_submit_query_successfully(self):
        pass

    def test_submit_query_failed_due_to_invalid_query(self):
        pass

    def test_validate_sql_successfully(self):
        pass

    def test_validate_sql_fail_due_to_invalid_sql(self):
        pass

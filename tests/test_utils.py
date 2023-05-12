import pytest
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestUtils:
    def test_create_failure_logs_successfully(self):
        pass

    def test_create_failure_logs_fail_due_to_invalid_failure_logs(self):
        pass

    def test_copy_file_successfully(self):
        pass

    def test_copy_file_fail_due_to_invalid_file(self):
        pass

    def test_remove_non_alphanumeric_successfully(self):
        pass

    def test_remove_non_alphanumeric_failed_due_to_all_non_alphanumeric(self):
        pass

    def test_get_latest_file_successfully(self):
        pass

    def test_get_latest_file_fail_due_to_value_error(self):
        pass

import os
import config
from pathlib import Path
import logging
import pytest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestGit:
    def test_push_to_git_successfully(self):
        pass

    def test_push_to_git_fail_due_to_non_existent_repo(self):
        pass

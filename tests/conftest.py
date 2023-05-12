import os
import pytest

pytest.fixture(scope='module', autouse=True)
def run_setup():
    os.system("python3 setup.py",
              "python3 main.py")

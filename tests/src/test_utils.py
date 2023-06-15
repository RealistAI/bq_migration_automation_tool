import logging
import os

import pytest
from google.cloud import bigquery

import config
import utils

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestRemoveNonAlphanumeric:
    def test_remove_non_alphanumeric(self):
        dirty_string = 'r3!m@03v$3d_a&1&(1_)n)0(n*_a1p!ha#n@u@m3!r$1^c^'
        clean_string = utils.remove_non_alphanumeric(string=dirty_string)

        assert clean_string == 'r3m03v3da11n0na1phanum3r1c'


class TestCreatePathIfNotExists:
    def test_if_not_exists(self):
        directory = f'not_a_existing_directory'
        command = f'rm -rf {directory}'
        if os.path.isdir(directory):
            os.system(command)

        utils.create_path_if_not_exists(path=directory)

        assert os.path.isdir(directory)

        os.system(command)


class TestGit:
    def test_push_to_git_successfully(self):
        commit_message = 'Adding transpiled and validated GoogleSQL to the repository'
        branch_name = utils.push_to_git(remote_repo=config.UC4_SQL_REPO,
                                        commit_message=commit_message)
        assert branch_name

    def test_push_to_git_fail_due_to_non_existent_repo(self):
        commit_message = "basic commit message"
        with pytest.raises(Exception):
            utils.push_to_git(remote_repo="not a real repository",
                              commit_message=commit_message)

    def test_get_path_from_git_repo_successfully(self):
        repo_name = "https://github.com/RealistAI/bq_migration_automation_tool.git"
        get_path = utils.get_path_from_git_repo(repo_dir=repo_name)
        assert get_path == "bq_migration_automation_tool"

    def test_get_repo_successfully(self):
        repo = config.UC4_SQL_REPO
        base_path = config.BASE_PATH
        utils.get_git_repo(repo=repo,
                           base_path=base_path)
        is_directory = os.path.isdir(str(config.UC4_SQL_REPO_NAME))
        assert is_directory


class TestExtractSQLDependencies:
    def test_ideal_conditions(self, uc4_job_list):
        sql_dependencies = utils.extract_sql_dependencies(sql_dependencies=uc4_job_list)

        assert type(sql_dependencies) == list


class TestGetUc4Json:
    def test_ideal_conditions(self):
        uc4_job = 'UC4_JOB_2'
        sql_dependencies_dictionary = utils.get_uc4_json(client=bigquery.Client(),
                                                         uc4_job_name=uc4_job)

        assert type(sql_dependencies_dictionary) == list
        assert type(sql_dependencies_dictionary[0]) == dict


@pytest.fixture
def file():
    file_name = f'fake.csv'
    with open(file_name, 'w') as file:
        file.write('File this')

    yield file_name

    os.system(f'rm {file_name}')


@pytest.fixture
def uc4_job_list():
    uc4_list = utils.get_uc4_json(client=bigquery.Client(),
                                  uc4_job_name="UC4_JOB_2")
    return uc4_list

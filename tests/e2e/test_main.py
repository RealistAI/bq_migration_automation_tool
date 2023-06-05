import config
import pytest
import logging
import time
import os
from pathlib import Path
from utils import utils, gcp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestE2e:
    def test_e2e(self,
                 create_directories):
        # Clearing out local directories and buckets
        os.system('./prerun.sh')
        #os.system(f'rm -rf ~/{config.BASE_PATH}/*')
        # Run make run to run the setup and main functions.
        os.system('make run')
        # assert transpiled and validated SQL are in the git repo
        time.sleep(5)
        assert is_repo_pushed(f'{config.BASE_PATH}/UC4_SQL/')
        assert is_table_populated(project_id=config.PROJECT, dataset_id=config.DATASET) != None
        print(f"{config.TARGET_SQL_PATH}/teradata_sql.sql")
        is_valid = os.path.isfile(f"{config.TARGET_SQL_PATH}/sql_1.sql")
        assert is_valid == True
        is_valid2 = os.path.isfile(f"{config.TARGET_SQL_PATH}/sql_2.sql")
        assert is_valid2 == True

def is_table_populated(project_id,
                       dataset_id):
    data = f"""
           SELECT * FROM {config.PROJECT}.{config.DATASET}.transpilation_logs
           """
    try:
        query_job = submit_query(query=data,
                                 dry_run=False).results()
        return query_job

    except Exception as error:
        return error

def is_repo_pushed(repo_path):
    # Change to the repository directory
    repo_path = os.path.abspath(repo_path)
    os.chdir(repo_path)

    # Check if the repository is a valid Git repository
    if not os.path.isdir(os.path.join(repo_path, '.git')):
        raise ValueError(f"{repo_path} is not a Git repository.")

    # Iterate over local branches
    branches_output = os.popen('git for-each-ref --format=%(refname:short) refs/heads').read().strip()
    branches = branches_output.split('\n')

    for branch in branches:
        # Check if the branch has a corresponding remote branch
        tracking_branch_output = os.popen('git rev-parse --abbrev-ref --symbolic-full-name @{u}').read().strip()
        if tracking_branch_output:
            local_commits_output = os.popen(f'git rev-list --count {branch}').read().strip()
            remote_commits_output = os.popen(f'git rev-list --count {branch}..{tracking_branch_output}').read().strip()

            local_commits = local_commits_output
            remote_commits = remote_commits_output

            # Compare the number of commits between local and remote branches
            if local_commits > remote_commits:
                return False

    return True

@pytest.fixture(scope="session")
def create_directories():
    root = Path(os.getcwd())
    utils.create_path_if_not_exists(config.E2E_OUTPUT)
    os.system(f'echo "SELECT * FROM \`michael-gilbert-dev.UC4_Jobs.uc4_to_sql_map\` LIMIT 1000" > {config.E2E_OUTPUT}/sql_1.sql')
    os.system(f'echo "SELECT job FROM \`michael-gilbert-dev.UC4_Jobs.uc4_to_sql_map\` LIMIT 1000" > {config.E2E_OUTPUT}/sql_2.sql')
    yield
    os.system("""
              cd ~/git/bq_migration_automation_tool;
              rm -r output/;
              """)

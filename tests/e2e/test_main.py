import config
import pytest
import logging
import time
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestE2e:
    def test_e2e(self):
        # Clearing out local directories and buckets
        os.system('./prerun.sh')
        os.system(f'rm -rf ~/{config.BASE_PATH}/*')
        # Run make run to run the setup and main functions.
        os.system('make run')
        # assert transpiled and validated SQL are in the git repo
        time.sleep(5)
        assert is_repo_pushed(f'{config.BASE_PATH}/UC4_SQL/')


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

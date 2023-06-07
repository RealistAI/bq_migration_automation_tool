import os
import config
import setup
import datetime
from utils import utils
from utils import git
from pathlib import Path
import re
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def push_to_git(remote_repo,
                commit_message) -> str:
    """
    creates a unique branch name and pushes the changes to the GitHub Repository.

    Args:
    remote_repo: The GitHub Repository that the changes are being pushed to.
    commit_message: The commit message that is being pushed to the GitHub Repository alone with the necessary changes.
    """
    current_datetime = str(datetime.datetime.now())
    stripped_current_datetime = utils.remove_non_alphanumeric(string=current_datetime)
    branch_name = f'bq_migration_tool_batch_{stripped_current_datetime}'
    base_path = config.BASE_PATH

    current_directory = os.getcwd()
    os.chdir(base_path)
    repo_directory_name = git.get_path_from_git_repo(remote_repo['path'])

    assert repo_directory_name is not None, \
        f"'{remote_repo['path']}' is not a valid git repo."

    if os.path.exists(Path(base_path, repo_directory_name)):
        os.chdir(repo_directory_name)
        os.system(f'git checkout -b {branch_name}')
        os.system(f'git add .')
        os.system(f'git commit -m "{commit_message}"')
        os.system(f'git push --set-upstream origin {branch_name}')

    os.chdir(current_directory)
    return branch_name


def get_path_from_git_repo(repo_dir: str) -> str | None:
    """
    Given a repo like https://github.com/RealistAI/UC4_SQL.git return
    UC4_SQL

    Args:
    repo_dir:  the full GitHub path of the repo that we are using to extract just the repository name.
    """
    match = re.search(r'[a-zA-Z-0-9_]*(?=\.git)', repo_dir)

    if match:
        return match.group()

    return None


def get_git_repo(repo: dict,
                 base_path: Path) -> None:
    """
    Given a repo name and a base path, get the latest version of the git repo
    If the repo is already downloaded to the current file system, pull the
    latest version

    Args:
    repo: Name of the GitHub repository you want to pull from
    base_path: The path leading to the GitHub Repository
    """
    current_dir = os.getcwd()
    os.chdir(base_path)

    repo_dir_name = get_path_from_git_repo(repo['path'])
    assert repo_dir_name is not None, \
        f"'{repo['path']}' is not a valid git repo."

    if os.path.exists(Path(base_path, repo_dir_name)):
        os.chdir(repo_dir_name)
        os.system(f"git checkout {repo['branch']}")
        os.system("git pull")
    else:
        os.system(f"git clone {repo['path']}")
        os.chdir(repo_dir_name)
        os.system(f"git checkout {repo['branch']}")

    os.chdir(current_dir)

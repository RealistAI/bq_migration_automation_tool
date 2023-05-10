import config
from pathlib import Path
import re
import os

def create_path_if_not_exists(path: Path):
    """
    Create the file path if it does not exist
    """
    if not os.path.exists(path):
        os.makedirs(path)

def get_path_from_git_repo(repo_dir: str) -> str|None:
    """
    Given a repo like https://github.com/RealistAI/UC4_SQL.git return
    UC4_SQL
    """
    match = re.search(r'[a-zA-Z-0-9_]*(?=\.git)',  repo_dir)

    if match:
        return match.group()

    return None


def get_git_repo(repo: dict, base_path: Path):
    """
    Given a repo name and a base path, get the latest version of the git repo
    If the repo is already downoaded to the current file system, pull the
    latest version
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

def setup():
    """
    Download the required repos and set the environment variables
    """

    create_path_if_not_exists(config.BASE_PATH)

    get_git_repo(repo=config.DWH_MIGRATION_TOOL_REPO,
                 base_path=config.BASE_PATH)

    get_git_repo(repo=config.UC4_SQL_REPO,
                 base_path=config.BASE_PATH)
if __name__ == "__main__":
    setup()

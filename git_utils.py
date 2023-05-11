import os
import config
import setup
import datetime
import utils
from pathlib import Path

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def push_to_git(local_repo,
                remote_repo,
                username,
                token,
                commit_message):
    current_datetime = str(datetime.datetime.now())
    stripped_current_datetime = utils.remove_non_alphanumeric(string=current_datetime)
    branch_name = f'bq_migration_tool_batch_{stripped_current_datetime}'
    base_path = config.BASE_PATH

    current_directory = os.getcwd()
    os.chdir(base_path)
    repo_directory_name = setup.get_path_from_git_repo(remote_repo['path'])

    assert repo_directory_name is not None, \
        f"'{repo['path']}' is not a valid git repo."

    if os.path.exists(Path(base_path, repo_directory_name)):
        os.chdir(repo_directory_name)
        os.system(f'git checkout -b {branch_name}')
        os.system(f'git add .')
        os.system(f'git commit -m "{commit_message}"')
        os.system(f'git push --set-upstream origin {branch_name}')
        # os.system(f'{username}')
        # os.system(f'{token}')

    os.chdir(current_directory)
    return branch_name


import config
from pathlib import Path
import re
import os
from utils import git, utils


def setup():
    """
    Download the required repos and set the environment variables
    """
    utils.create_path_if_not_exists(config.BASE_PATH)

    git.get_git_repo(repo=config.DWH_MIGRATION_TOOL_REPO,
                     base_path=config.BASE_PATH)

    git.get_git_repo(repo=config.UC4_SQL_REPO,
                     base_path=config.BASE_PATH)

    utils.create_path_if_not_exists(config.TARGET_SQL_PATH)


if __name__ == "__main__":
    setup()

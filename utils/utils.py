import json
import logging
import os
from pathlib import Path

import config
from utils import gcp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_path_if_not_exists(path) -> None:
    """
    Create the file path if it does not exist

    Args:
    path: the file path we are creating if it doesn't exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def get_uc4_json(project_id: str,
                 dataset_id: str,
                 uc4_job_name: str) -> dict:
    """
    Runs a query to attain the json data located in the uc4_json table for a specific uc4_job.

    Args:
    project_id: the project_id used in conjunction with the dataset_id to access the table.
    dataset_id: the dataset_id used in conjunction with the project_id to access the table.
    uc4_job_name: the name of the uc4_job we want the json data from.
    """

    # get the json for this uc4 job from BigQuery
    json_data_query = f"SELECT json_data FROM {project_id}.{dataset_id}.uc4_json WHERE job_id = {uc4_job_name}"
    json_data_query_results = gcp.submit_query(query=json_data_query,
                                               dry_run=False)

    # Convert the JSON to a Dict
    for row in json_data_query_results:
        json_data = row[0]
        dependency_dict = json.loads(json_data)

        # Return it 
        return dependency_dict


def get_sql_dependencies(uc4_job: dict,
                         repo_path: Path) -> list:
    """
    Parse the uc4_job Dict to get the SQL dependencies
    Find those SQL files in the repo_path
    Read those files and append them to a list

    Args:
    uc4_job: the dictionary containing the paths to the sql_files.
    repo_path: the path to the repo that contains the sql_files.
    """

    sql_strings = []
    # Extract the SQL Dependencies from the uc4_job Dict
    sql_dependencies = uc4_job['sql_dependencies']
    sql_path = extract_sql_dependencies(sql_dependencies)

    # Iterate through them, read the file, and append that to a list
    for file_path in sql_path:
        print("file_path is ", file_path)
        print("\n")
        if len(file_path) == 0:
            pass
        else:
            file_directory = file_path.split("/")
            file_directory = file_directory[-2]
            print("file_directory is ", file_directory)
            print("\n")
            repo_path = str(repo_path) + "/" + file_directory
            print("new repo path is", repo_path)
            print("\n")
            file_name = os.path.basename(file_path)
            create_path_if_not_exists(repo_path)
            with open(Path(repo_path, file_name), 'r') as sql_file:
                sql_strings.append(sql_file.read())
            repo_path = config.SOURCE_SQL_PATH

    print("list of sql dependencies is ", sql_strings)
    return sql_strings


def extract_sql_dependencies(sql_dependencies: list):
    """
    Extracts all the sql_dependencies/sql_file_paths from within json data from bigquery.

    Args:
    sql_dependencies: The list containing the nested dictionaries which contain all the
                      sql_dependencies such as the sql_file_paths needed for transpilation and validation.
    """
    sql_paths = []
    for dependencies in sql_dependencies:
        if dependencies.get('sql_dependencies'):
            sql_paths.extend(extract_sql_dependencies(dependencies['sql_dependencies']))
        else:
            sql_file_path = dependencies.get("sql_file_path")
            sql_paths.append(sql_file_path)
    return sql_paths


def remove_non_alphanumeric(string):
    """ Removes all characters that are not numbers or letters

    Args:
    string: The string you wish to remove non alphanumeric characters from.
    """
    alphanumeric_chars = []
    for char in string:
        if char.isalnum():
            alphanumeric_chars.append(char)
    return ''.join(alphanumeric_chars)

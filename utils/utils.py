from pathlib import Path
from google.api_core import exceptions as gcp_exceptions
import config
import datetime
import yaml
import csv
import os
import logging
import json
from utils import gcp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_path_if_not_exists(path) -> None:
    """
    Create the file path if it does not exist

    Args:
    path: the file path we are creating if it doesnt exist.
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
    json_data_query = f"SELECT json_data FROM {project_id}.{dataset_id}.uc4_json WHERE job_id = '{uc4_job_name}'"
    json_data_query_results = gcp.submit_query(query=json_data_query,
                                                   dry_run="False")

    # Convert the JSON to a Dict
    for row in json_data_query_results:
        json_data = row[0]
        dependency_dict = json.loads(json_data)

        # Return it 
        return dependency_dict

def get_sql_dependencies(uc4_job: dict,
                         repo_path: Path) -> list():
    """
    Parse the uc4_job Dict to get the SQL dependencies
    Find those SQL files in the repo_path
    Read those files and append them to a list

    Args:
    uc4_job: the dictionary containing the paths to the sql_files.
    repo_path: the path to the repo that contains the sql_files.
    """

    sql = []
    # Extract the SQL Dependencies from the uc4_job Dict
    sql_dependencies = uc4_job['sql_dependencies']
    workflow = {}
    sql_path = extract_sql_dependencies(sql_dependencies)

    # Iterate through them, read the file, and append that to a list
    number = 1
    for items in sql_path:
        workflow[number] = items
        with open(Path(repo_path, items), 'r') as sql_file:
            sql.append(sql_file.read())
        number += 1

    run_order = {'uc4_job_name': job, 'sql_path': workflow}
    return sql

def extract_sql_dependencies(sql_dependencies: list):
    """
    extracts all the sql_dependencies/sql_file_paths from within json data from bigquery.(a list with nested dictionarys)

    Args:
    sql_dependencies: The list containing the nested dictionaries which contain all the sql_dependencies such as the sql_file_paths needed for transpilation and validation.
    """
    sql_paths = []
    for dependencies in sql_dependencies:
        if dependencies.get('sql_dependencies'):
            sql_paths.extend(extract_sql_dependencies(dependencies['sql_dependencies']))
        else:
            sql_file_path = dependencies.get("sql_file_path")
            sql_paths.append(sql_file_path)
    return sql_paths





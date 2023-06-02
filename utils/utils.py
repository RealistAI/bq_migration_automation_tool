from pathlib import Path
from google.api_core import exceptions as gcp_exceptions
import config
import datetime
import yaml
import csv
import os
import logging
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
`
def get_uc4_json(project_id: str, dataset: str, uc4_job_name: str) -> dict:

    # get the json for this uc4 job from BigQuery
    json_data_query = f"SELECT json_data FROM {project_id}.{dataset_id}.uc4_json WHERE job_id = '{uc4_job_name}'"
    json_data_query_results = gcp.submit_query(query=json_data_query,
                                                   dry_run="False")

    # Convert the JSON to a Dict
    for row in json_data_query_results:
        json_data = row[0]
        dependency_dict = json.loads(json_data)
        sql_dependencies = dependency_dict['sql_dependencies']
        workflow = {}
        number = 1
        for items in sql_path:
            workflow[number] = items
            number += 1

        run_order = {'uc4_job_name': job, 'sql_path': workflow}

        list_of_uc4_jobs.append(run_order)

    # Return it
    print(f"list of uc4 jobs: {list_of_uc4_jobs}")
    return list_of_uc4_jobs


def get_sql_dependencies(uc4_job: dict, repo_path: Path) -> List[str]:
    """
    Parse the uc4_job Dict to get the SQL dependencies
    Find those SQL files in the repo_path
    Read those files and append them to a list
    """

    sql = []
    # Extract the SQL Dependencies from the uc4_job dict

    # Iterate through them, read the file, and append that to a list

    for i in sql_dependencies:
        with open(Path(repo_path, i), 'r') as sql_file:
            sql.append(sql_file.read())

    return sql

list_of_uc4_jobs = []
    csv_of_job_names = open("uc4_jobs.csv", "r").readlines()
    number_of_jobs = 0

    for job in csv_of_job_names:
        # Get the JSON for that job after parsing through job names
        job = job.replace("\"", "").split(",")
        job = job[number_of_jobs]
        json_data_query = f"SELECT json_data FROM {project_id}.{dataset_id}.uc4_json WHERE job_id = '{job}'"
        json_data_query_results = gcp.submit_query(query=json_data_query,
                                                   dry_run="False")
        number_of_jobs += 1

        for row in json_data_query_results:
            json_data = row[0]
            dependency_dict = json.loads(json_data)
            sql_dependencies = dependency_dict['sql_dependencies']
            workflow = {}
            sql_path = extract_sql_dependencies(sql_dependencies)
            number = 1
            for items in sql_path:
                workflow[number] = items
                number += 1

            run_order = {'uc4_job_name': job, 'sql_path': workflow}

            list_of_uc4_jobs.append(run_order)
    print(f"list of uc4 jobs: {list_of_uc4_jobs}")
    return list_of_uc4_jobs

def extract_sql_dependencies(sql_dependencies):
    sql_paths = []
    for dependencies in sql_dependencies:
        if dependencies.get('sql_dependencies'):
            sql_paths.extend(extract_sql_dependencies(dependencies['sql_dependencies']))
        else:
            sql_file_path = dependencies.get("sql_file_path")
            sql_paths.append(sql_file_path)
    return sql_paths





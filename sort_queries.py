from google.cloud import bigquery
from utils import gcp, utils
import config
from pathlib import Path
import os
import logging
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_uc4_jobs_table(project_id,
                          dataset_id) -> None:
    """
    Uses the project_id and dataset_id to create the uc4_json table.

    Args:
    project_id: the project being used to create the uc4_json table.
    dataset_id: the dataset being used to create the uc4_json table.
    """
    client = bigquery.Client()
    try:
        create_uc4_table = client.query(f"""
                                           CREATE TABLE IF NOT EXISTS {project_id}.{dataset_id}.uc4_json (
                                           job_id STRING,
                                           json_data JSON);
                                        """)

        results = create_uc4_table.result()

        for row in results:
            print(f"{row.url} : {row.view_count}")

    except Exception as error:
        print(error)

def sort_queries(project_id,
                 dataset_id) -> None:
    """
    Runs a query to extract the DISTINCT Jobs from BQ and then another query to attain the specific SQLs from those DISTINCT jobs. Then the specific SQLs of the same job will be added to the same `.sql` file to be validated by `main.py`.

    Args:
    project: the project being used to access the uc4_to_sql_map table.
    dataset: the dataset being used to access the uc4_to_sql_map table.
    """
    list_of_uc4_jobs = []
    distinct_job_query = f"SELECT DISTINCT job_id FROM {project_id}.{dataset_id}.uc4_json ORDER BY job_id"
    try:
        distinct_job_query_results = gcp.submit_query(query=distinct_job_query,
                                                      dry_run="False")
    except Exception as error:
        return error

    for job in distinct_job_query_results:
        # Get all of the SQLs for that job
        job = job[0]
        json_data_query = f"SELECT json_data FROM {project_id}.{dataset_id}.uc4_json WHERE job_id = '{job}'"
        json_data_query_results = gcp.submit_query(query=json_data_query,
                                                   dry_run="False")

        for row in json_data_query_results:
            json_data = row[0]
            dependency_dict = json.loads(json_data)
            print(f"dictionary is {dependency_dict}")
            dependencies = dependency_dict['dependencies']
            job_name = dependency_dict['job_name']
            workflow = {}

            for dependency in dependencies:
                sql_path = dependency['sql_file_path']
                step = dependency['order']
                workflow[step] = sql_path

            run_order = {'uc4_job_name': job_name, 'sql_path': workflow}

            list_of_uc4_jobs.append(run_order)
    print(f"list of uc4 jobs: {list_of_uc4_jobs}")
    return list_of_uc4_jobs



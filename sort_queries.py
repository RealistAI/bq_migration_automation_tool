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

create_uc4_jobs_table(config.PROJECT, config.DATASET)

def sort_queries(project_id,
                 dataset_id) -> None:
    """
    Runs a query to extract the DISTINCT Jobs from BQ and then another query to attain the specific SQLs from those DISTINCT jobs. Then the specific SQLs of the same job will be added to the same `.sql` file to be validated by `main.py`.

    Args:
    project: the project being used to access the uc4_to_sql_map table.
    dataset: the dataset being used to access the uc4_to_sql_map table.
    """
    list_of_uc4_jobs = []
    csv_of_job_names = open("uc4_jobs.csv", "r").readlines()
    #distinct_job_query = f"SELECT DISTINCT job_id FROM {project_id}.{dataset_id}.uc4_json ORDER BY job_id"
    #try:
    #    distinct_job_query_results = gcp.submit_query(query=distinct_job_query,
    #                                                  dry_run="False")
    #except Exception as error:
    #    return error

    #for job in distinct_job_query_results:
    number_of_jobs = 0
    for job in csv_of_job_names:
        job = job.replace("\"", "").split(",")
        job = job[number_of_jobs]
        print("job is ", job)
        # Get all of the SQLs for that job
        json_data_query = f"SELECT json_data FROM {project_id}.{dataset_id}.uc4_json WHERE job_id = '{job}'"
        print(json_data_query)
        json_data_query_results = gcp.submit_query(query=json_data_query,
                                                   dry_run="False")
        number_of_jobs += 1

        for row in json_data_query_results:
            json_data = row[0]
            dependency_dict = json.loads(json_data)
            print(f"dictionary is {dependency_dict}\n")
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
    for dependency in sql_dependencies:
        print("Dependency is ", dependency)
        print("\n")
        if dependency.get('sql_dependencies'):
            sql_paths.extend(extract_sql_dependencies(dependency['sql_dependencies']))
        else:
            sql_file_path = dependency.get("sql_file_path")
            sql_paths.append(sql_file_path)
    return sql_paths

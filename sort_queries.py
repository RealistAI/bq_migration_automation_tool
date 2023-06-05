from google.cloud import bigquery
from utils import gcp
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
            logger.info(f"{row.url} : {row.view_count}")

    except Exception as error:
        logger.info(error)

#create_uc4_jobs_table(config.PROJECT, config.DATASET)

def sort_queries(project_id,
                 dataset_id) -> None:
    """
    Loads a CSV file full of the uc4_jobs, then runs a query to attain the specific JSON from those jobs. Thn all the jobs will be added to a list of dictionaries, where each job and its sqls will be added to its own dictionary to be validated by `main.py`.

    Args:
    project: the project being used to access the uc4_to_sql_map table.
    dataset: the dataset being used to access the uc4_to_sql_map table.
    """
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

    logger.info(f"list of uc4 jobs: {list_of_uc4_jobs}")
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

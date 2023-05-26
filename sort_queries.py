from google.cloud import bigquery
from utils import gcp, utils
import config
from pathlib import Path
import os
import logging

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
    distinct_job_query = f"SELECT DISTINCT job_id FROM {project_id}.{dataset_id}.uc4_json ORDER BY job_id"
    try:
        distinct_job_query_results = gcp.submit_query(query=distinct_job_query,
                                                      dry_run="False")
    except Exception as error:
        return error

    for row in distinct_job_query_results:
        sql_path_data = {}
        uc4_jobs = {}
        # Get all of the SQLs for that job
        job = row[0]
        sql_path_query = f"SELECT json_data.dependencies[0]['sql_file_path'] AS sql_file_path, json_data.dependencies[1]['sql_file_path'] AS sql_file_path2 FROM {project_id}.{dataset_id}.uc4_json WHERE job_id = '{job}'"
        sql_path_query_results = gcp.submit_query(query=sql_path_query,
                                                  dry_run="False")

        order_of_queries = 0
        for row in sql_path_query_results:
            # Append that SQL to our temp SQL file
            sql_path = row[0]
            second_sql_path = row[1]
            print(f"Path is: {sql_path}")
            print(f"second path is: {second_sql_path}")
            order_of_queries += 1
            sql_path_data[order_of_queries] = sql_path
            order_of_queries += 1
            sql_path_data[order_of_queries] = second_sql_path
            print(f"File data is : {sql_path_data}")


        uc4_jobs["uc4_job_name"] = job
        uc4_jobs["sql_paths"] = sql_path_data
        list_of_uc4_jobs.append(uc4_jobs)
    print(f"list of uc4 jobs: {list_of_uc4_jobs}")
    return list_of_uc4_jobs


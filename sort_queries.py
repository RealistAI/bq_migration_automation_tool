from google.cloud import bigquery
from utils import gcp, utils
import config
from pathlib import Path
import os
import logging
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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
        dependency_dict = utils.get_uc4_json(project_id=project_id, dataset_id=dataset_id, uc4_job_name=job)
        number_of_jobs += 1

        sql_dependencies = dependency_dict['sql_dependencies']
        workflow = {}
        sql_path = utils.extract_sql_dependencies(sql_dependencies)
        number = 1
        for items in sql_path:
            workflow[number] = items
            number += 1

        run_order = {'uc4_job_name': job, 'sql_path': workflow}
        list_of_uc4_jobs.append(run_order)

    logger.info(f"list of uc4 jobs: {list_of_uc4_jobs}")
    print(f"list of uc4 jobs: {list_of_uc4_jobs}")
    return list_of_uc4_jobs

from utils import gcp, utils
import config
from pathlib import Path
import os
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def sort_queries(project_id,
                 dataset_id) -> None:
    """
    Runs a query to extract the DISTINCT Jobs from BQ and then another query to attain the specific SQLs from those DISTINCT jobs. Then the specific SQLs of the same job will be added to the same `.sql` file to be validated by `main.py`.

    Args:
    project: The project being used to access the uc4_to_sql_map table.
    dataset: the dataset being used to access the uc4_to_sql_map table.
    """
    uc4_jobs = list()
    distinct_job_query = f"SELECT DISTINCT job FROM {project_id}.{dataset_id}.uc4_to_sql_map ORDER BY job"
    try:
        distinct_job_query_results = gcp.submit_query(query=distinct_job_query,
                                                      dry_run="False")
    except Exception as error:
         return error

    for row in distinct_job_query_results:
        sql_data = list()
        # Get all of the SQLs for that job
        job = row[0]
        sql_path_query = f"SELECT sql_path FROM {project_id}.{dataset_id}.uc4_to_sql_map WHERE job = '{job}' ORDER BY order_of_queries"
        sql_path_query_results = gcp.submit_query(query=sql_path_query,
                                                  dry_run="False")

        for row in sql_path_query_results:
            # Append that SQL to our temp SQL file
            sql_path = row[0]
            print(f"Path is: {sql_path}")
            with open(sql_path, 'r') as sql_file:
                sql = sql_file.read()
                sql_data.append(sql)
                print(f"File data is : {sql_data}")

        uc4_jobs.append(sql_data)
    print(f"uc4 jobs: {uc4_jobs}")
    return uc4_jobs


from utils import gcp
import config

def sort_queries(project,
                 dataset) -> None:
    """
    Runs a query to extract the DISTINCT Jobs from BQ and then another query to attain the specific SQLs from those DISTINCT jobs. Then the specific SQLs of the same job will be added to the same `.sql` file to be validated by `main.py`.

    Args:
    project: The project being used to access the uc4_to_sql_map table.
    dataset: the dataset being used to access the uc4_to_sql_map table.
    """
    sql_data = list()
    distinct_job_query = f"SELECT DISTINCT job FROM {project}.{dataset}.uc4_to_sql_map"
    try:
        distinct_job_query_results = gcp.submit_query(query=distinct_job_query,
                                                  dry_run="False")
    except Exception as error:
         return error

    for row in distinct_job_query_results:
        # Get all of the SQLs for that job
        job = row[0]
        sql_path_query = f"SELECT sql_path FROM {project}.{dataset}.uc4_to_sql_map WHERE job = '{job}'"
        sql_path_query_results = gcp.submit_query(query=sql_path_query,
                                                  dry_run="False")

        for row in sql_path_query_results:
            # Get the SQL from the repo
            # Append that SQL to our temp SQL file
            sql_path = row[0]
            with open(sql_path, 'r') as sql_file:
                sql_data += sql_file.read()

    # We have all of the SQL files for this job
    # Submit sql_data to BigQuery for dry-run


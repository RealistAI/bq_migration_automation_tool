import logging
import os

import config
import sort_queries as s
import teradata_to_bq_dataset_mapping as tdm
from utils import gcp
from utils import git

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    """ Translates given SQL to Google dialect

    This functions takes the SQL in a given GitHub repository, clones
    it into the local file system, submits it to the BigQuery batch translation
    client, validates it all with dry-run queries, and finally opens a branch
    in the given GitHub repository, and pushes the validated sql to that new branch.
    """
    failures = 0

    # Ensure intermediary GCS buckets and directories are empty and run BQMS

    os.system(f'''
    ./prerun.sh
    export BQMS_PROJECT={config.PROJECT};
    export BQMS_PREPROCESSED_PATH={config.PREPROCESED_BUCKET};
    export BQMS_INPUT_PATH={config.SOURCE_SQL_PATH};
    export BQMS_TRANSLATED_PATH={config.TRANSLATED_BUCKET};
    export BQMS_POSTPROCESSED_PATH={config.SQL_TO_VALIDATE};
    export BQMS_CONFIG_PATH={config.CONFIG_YAML};
    export BQMS_VERBOSE={config.DEBUG};
    export BQMS_OBJECT_NAME_MAPPING_PATH={config.OBJECT_MAPPING};
    bqms-run
    ''')

    # Iterate through BQMS output and validate transpiled SQL
    list_of_uc4_jobs = s.sort_queries(config.PROJECT, config.DATASET)
    for uc4_job in list_of_uc4_jobs:
        print(f"\nExtracting details about UC4 JOB {uc4_job['uc4_job_name']}\n")
        steps = uc4_job['sql_path']
        job_name = uc4_job['uc4_job_name']
        # This gives you the job name on one variable a way to order the queries and their paths
        for i in range(1, len(steps) + 1):
            path_to_query = steps[i]
            file_name = path_to_query.split("/")
            file_name = file_name[-1]
            print("file name is ", file_name)
            # Then you have the path to the sql for each consecutive step
            is_valid = gcp.validate_sql(sql_to_validate=path_to_query, uc4_job_name=job_name)
            logger.info(f"Validate SQL returned: {is_valid}")

            # If SQL in file is valid copy it into UC4_SQL_REPO/bigquery_sql/
            if is_valid is True:
                logger.info(f'{job_name} validated and added to {config.TARGET_SQL_PATH}')
                os.system(f'cp {config.SQL_TO_VALIDATE}/{file_name} {config.TARGET_SQL_PATH}/')

    message = f'Completed transpilation process, to read logs see: {config.LOGGING}'
    logger.info(message)

    repo_directory_name = git.get_path_from_git_repo(repo_dir=config.UC4_SQL_REPO['path'])

    logger.info(f'Pushing validated SQL to {repo_directory_name}')
    commit_message = f'Adding transpiled and validated GoogleSQL to the {repo_directory_name}'

    git.push_to_git(remote_repo=config.UC4_SQL_REPO, commit_message=commit_message)


if __name__ == "__main__":
    main()
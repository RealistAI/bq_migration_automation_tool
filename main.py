import os
import config
import utils.utils
from utils import gcp
from utils import git
from pathlib import Path
import transpilation_logs as tl
import setup
import sort_queries as s
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    ''' Translates given SQL to Google dialect

    This functions takes the SQL in a given github repository, clones
    it into the local file system, submits it to the BigQuery batch translation
    client, validates it all with dry-run queries, and finally opens a branch
    in the given github repository, and pushes the validated sql to that new branch.
    '''
    failures = 0

    # Ensure intermediary GCS buckets and directories are empty and run BQMS
    os.system(f'''
    ./prerun.sh;
    export BQMS_PROJECT={config.PROJECT};
    export BQMS_PREPROCESSED_PATH={config.PREPROCESED_BUCKET};
    export BQMS_INPUT_PATH={config.SOURCE_SQL_PATH};
    export BQMS_TRANSLATED_PATH={config.TRANSLATED_BUCKET};
    export BQMS_POSTPROCESSED_PATH={config.SQL_TO_VALIDATE};
    export BQMS_CONFIG_PATH={config.CONFIG};
    export BQMS_VERBOSE={config.DEBUG};
    bqms-run
    ''')

    # Iterate through BQMS output and validate transpiled SQL
    uc4_jobs = s.sort_queries(config.PROJECT, config.DATASET)
    uc4_jobs = uc4_jobs.items()
    for job in uc4_jobs:
        logger.info(f'Validating {job[1]} for uc4 job {job[0]}')
        is_valid = gcp.validate_sql(sql_strings=job[1],
                                    uc4_chain_name=)

        # If SQL in file is valid copy it into UC4_SQL_REPO/bigquery_sql/
        if is_valid is True:
            os.system(f'cp {config.SQL_TO_VALIDATE}/{sql} {config.TARGET_SQL_PATH}/')
            logger.info(f'{job} validated and added to {config.TARGET_SQL_PATH}')
        else:
            failures += 1

    message = f'''\nAll files in {config.TARGET_SQL_PATH} have been processed with
    {failures} failed validations.'''
    logger.info(message)

    repo_directory_name = setup.get_path_from_git_repo(repo_dir=config.UC4_SQL_REPO['path'])

    logger.info(f'Pushing validated SQL to {repo_directory_name}')
    commit_message = f'Adding transpiled and validated GoogleSQL to the {repo_directory_name}'

    branch_name = git_utils.push_to_git(remote_repo=config.UC4_SQL_REPO,
                                        commit_message=commit_message)

if __name__ == "__main__":
    main()


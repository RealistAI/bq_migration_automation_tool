import os
import config
import utils
import gcp_utils
import git_utils
from pathlib import Path
import setup

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
    files_to_ignore = ['batch_translation_report.csv','consumed_name_map.json']
    os.system(f'mv {config.SQL_TO_VALIDATE}/batch_translation_report.csv translation_reports')
    for file_name in os.listdir(config.SQL_TO_VALIDATE):
        if file_name not in files_to_ignore:
            logger.info(f'Validating {file_name}')
            sql_file_to_validate = f'{config.SQL_TO_VALIDATE}/{file_name}'
            is_valid = gcp_utils.validate_sql(sql_to_validate=sql_file_to_validate,
                                              file_name=file_name)

            # If SQL in file is valid copy it into UC4_SQL_REPO/bigquery_sql/
            if is_valid is True:
                os.system(f'cp {config.SQL_TO_VALIDATE}/{file_name} {config.TARGET_SQL_PATH}/')
                logger.info(f'{file_name} validated and added to {config.TARGET_SQL_PATH}')
            else:
                failure_log_path = config.FAILURE_LOGS
                failure_log = utils.get_latest_file(failure_log_path)
                failures += 1

    message = f'''\nAll files in {config.TARGET_SQL_PATH} have been processed with 
    {failures} failed validations. For more details view {failure_log}\n'''
    logger.info(message)

    repo_directory_name = setup.get_path_from_git_repo(repo_dir=config.UC4_SQL_REPO['path'])

    logger.info(f'Pushing validated SQL to {repo_directory_name}')
    commit_message = f'Adding transpiled and validated GoogleSQL to the {repo_directory_name}'

    branch_name = git_utils.push_to_git(remote_repo=config.UC4_SQL_REPO,
                                        commit_message=commit_message)

if __name__ == "__main__":
    main()


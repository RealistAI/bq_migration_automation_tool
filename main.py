import os
#from config.config import bq_migration_config
import config
#import utils
#import gcp_utils
from pathlib import Path
import re

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    # This script deletes all files in validated sql prior to running. 
    # pull the latest sql from the uc4 sql repo
    os.system(f'''
    export BQMS_PROJECT={config.PROJECT};
    export BQMS_PREPROCESSED_PATH={config.PREPROCESED_BUCKET};
    export BQMS_INPUT_PATH={config.SOURCE_SQL_PATH};
    export BQMS_TRANSLATED_PATH={config.TRANSLATED_BUCKET};
    export BQMS_POSTPROCESSED_PATH={config.TARGET_SQL_PATH};
    export BQMS_CONFIG_PATH={config.CONFIG};
    export BQMS_VERBOSE={config.DEBUG};
    bqms-run
    ''')
    #failures = 0
    #gcp_project = bq_migration_config['project']
    #transpiled_sql = bq_migration_config['transpiled_sql']
    #source_sql = bq_migration_config['source_sql']
    ## name_mapping_config = {bq_migration_config['mapping_config']}
    #git_repo = bq_migration_config['uc4_git_repo']
    #utils.update_default_database(default_database=gcp_project,
    #                              dwh_config_path=bq_migration_config['dwh_config'])

    #os.system('rm -rf validated_sql/*')
    #for file_name in os.listdir('git_repo/'):
    #    logger.info(f'Processing {file_name}')
    #    file = f'git_repo/{file_name}'
    #    bqms_input = f'source_sql/{file_name}'
    #    os.system('./prerun.sh')

    #    utils.copy_file(path_of_file_to_copy=file,
    #                    path_to_target=bqms_input)

    #    os.system(f'''
    #    export BQMS_PROJECT={gcp_project};
    #    export BQMS_PREPROCESSED_PATH={bq_migration_config['preprocessed']};
    #    export BQMS_INPUT_PATH={source_sql};
    #    export BQMS_TRANSLATED_PATH={bq_migration_config['translated']};
    #    export BQMS_POSTPROCESSED_PATH={transpiled_sql};
    #    export BQMS_CONFIG_PATH={bq_migration_config['dwh_config']};
    #    export BQMS_VERBOSE={bq_migration_config['debug_mode']};
    #    bqms-run
    #    ''')

    #    logger.info('Batch Translation complete. Validating results.')
    #    # Run a BQ Dryun for each of the SQL's in each file in the transpiled_sql directory
    #    path_to_sql_to_validate = f'{transpiled_sql}{file_name}'
    #    is_valid = gcp_utils.validate_sql(sql_to_validate=path_to_sql_to_validate)

    #    if is_valid:
    #        utils.copy_file(path_of_file_to_copy=path_to_sql_to_validate,
    #        path_to_target=f'validated_sql/{file_name}')
    #    else:
    #        failures = failures+1

    #message = f'''All files in {git_repo} have been processed with 
    #{failures} failed validations. See invalid_sql/failure_logs.csv'''
    #logger.info(message)



# Git integration
# Create branch
# Commit and start PR
if __name__ == "__main__":
    main()


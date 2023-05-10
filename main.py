import os
import config
import gcp_utils
from pathlib import Path

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    # This script deletes all files in validated sql prior to running. 
    # pull the latest sql from the uc4 sql repo

    failures = 0

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

    files_to_ignore = ['batch_translation_report.csv','consumed_name_map.json']
    os.system(f'mv {config.SQL_TO_VALIDATE}/batch_translation_report.csv translation_reports')
    for file_name in os.listdir(config.SQL_TO_VALIDATE):
        if file_name not in files_to_ignore:
            logger.info(f'Validating {file_name}')
            sql_file_to_validate = f'{config.SQL_TO_VALIDATE}/{file_name}'
            is_valid = gcp_utils.validate_sql(sql_to_validate=sql_file_to_validate,
                                              file_name=file_name)

            if is_valid is True:
                os.system(f'cp {config.SQL_TO_VALIDATE}/{file_name} {config.TARGET_SQL_PATH}/')
                logger.info(f'{file_name} validated and added to {config.TARGET_SQL_PATH}')
            else:
                failures += 1

    message = f'''\nAll files in {config.TARGET_SQL_PATH} have been processed with 
    {failures} failed validations. See invalid_sql/failure_logs.csv\n'''
    logger.info(message)



# Git integration
# Create branch
# Commit and start PR
if __name__ == "__main__":
    main()


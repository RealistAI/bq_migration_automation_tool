import os
from config.config import bq_migration_config
import utils
import gcp_utils

def main():
    gcp_project = bq_migration_config['project']
    output_directory = bq_migration_config['output']
    input_directory = bq_migration_config['input']
    name_mapping_config = {bq_migration_config['mapping_config']}
    utils.update_default_database(default_database=gcp_project,
                                  dwh_config_path=bq_migration_config['dwh_config'])
    print(f"The name mapping config is {name_mapping_config}")

    os.system(f'''
    export BQMS_PROJECT={bq_migration_config['project']};
    export BQMS_PREPROCESSED_PATH={bq_migration_config['preprocessed']};
    export BQMS_INPUT_PATH={input_directory};
    export BQMS_TRANSLATED_PATH={bq_migration_config['translated']};
    export BQMS_POSTPROCESSED_PATH={output_directory};
    export BQMS_CONFIG_PATH={bq_migration_config['dwh_config']};
    export BQMS_VERBOSE={bq_migration_config['debug_mode']};
    export BQMS_OBJECT_NAME_MAPPING_PATH={name_mapping_config};
    bqms-run
    ''')
    #export BQMS_MACRO_MAPPING={};
    #export BQMS_MULTITHREAD=True;
    #export BQMS_GCS_CHECKS=False;

# Dryrun
# Run a BQ Dryun for each of the SQL's in each file in the transpiled_sql directory
    path_to_sql_to_validate = f'{output_directory}/sql_to_translate.txt'
    gcp_utils.validate_sql(sql_to_validate=path_to_sql_to_validate,
                           output_validated_sql=bq_migration_config['validated_sql_output'],
                           output_invalid_sql=bq_migration_config['invalid_sql_output'])

# Git integration
# Create branch
# Commit and start PR
if __name__ == "__main__":
    main()



import yaml

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def update_default_database(default_database, dwh_config_path):
    with open(dwh_config_path , 'r') as file:
        data = yaml.safe_load(file)
        data['default_database'] = f'{default_database}'

def write_list_to_file(file, list_to_write):
    with open (file, 'w') as file_to_write_to:
        for element in list_to_write:
            element_with_new_line = element + '\n'
            file_to_write_to.write(element_with_new_line)
    logger.info(f"Successfully added list elements to {file}")

def create_file(file_name):
    new_file = open(file_name, 'x')
    new_file.close()

    logger.info(f'Successfully created file "{file_name}"')




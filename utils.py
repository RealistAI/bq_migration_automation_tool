import yaml
import csv


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

def append_to_csv_file(csv_file_path:str,
                       data:list):
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['file_name','error_message'])
        writer.writerow(data)

def copy_file(path_of_file_to_copy,
              path_to_target):
    logger.info(f"Coping {path_of_file_to_copy} to {path_to_target}")
    with open (path_of_file_to_copy, 'r') as origin_file:
        data = origin_file.read()
    with open (path_to_target, 'w') as target_file:
        target_file.write(data)

def remove_non_alphanumeric(string):
    alphanumeric_chars = []
    for char in string:
        if char.isalnum():
            alphanumeric_chars.append(char)
    return ''.join(alphanumeric_chars)

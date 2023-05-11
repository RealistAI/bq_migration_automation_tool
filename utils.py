import yaml
import config
import csv


import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def update_default_database():
    # Updates the dwh config.yaml with the GCP project specified in the config.py
    with open(config.CONFIG , 'r') as file:
        data = yaml.safe_load(file)
        data['default_database'] = f'{config.PROJECT}'

def append_to_csv_file(csv_file_path:str,
                       data:list[str:str]):
    ''' Writes elements of a list to csv file 

    Writes a CSV file with the header 'file_name,error_message' and 
    then writes the provided strings in the row after.

    Args:
    csv_file_path: Path to the file you want to write
    data: A list containing a two elements
    '''
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['file_name','error_message'])
        writer.writerow(data)

def copy_file(path_of_file_to_copy,
              path_to_target):
    """ Copy a file from a given location to a given location

    Args:
    path_of_file_to_copy: The path to the file you want to copy
    path_to_target: Path to the directory you want to copy the file into
    """
    logger.info(f"Coping {path_of_file_to_copy} to {path_to_target}")

    try:
        with open (path_of_file_to_copy, 'r') as origin_file:
            data = origin_file.read()
    except Exception as error:
        logger.info(f'Unable to open {path_of_file_to_copy}')

    try:
        with open (path_to_target, 'w') as target_file:
            target_file.write(data)
    except Exception as error:
        logger.info(f'''Failed to write the data extracted from 
                    {path_of_file_to_copy} to {path_to_target}''')

def remove_non_alphanumeric(string):
    ''' Removes all characters that are not numbers or letters

    Args:
    string: The string you wish to remove non alphanumeric characters from.
    '''
    alphanumeric_chars = []
    for char in string:
        if char.isalnum():
            alphanumeric_chars.append(char)
    return ''.join(alphanumeric_chars)

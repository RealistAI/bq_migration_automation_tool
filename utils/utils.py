import yaml
import config
import csv
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_path_if_not_exists(path: Path) -> None:
    """
    Create the file path if it does not exist

    Args:
    path: the file path we are creating if it doesnt exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)

def create_failure_log(failure_log_file:str,
                       data:dict):
    ''' Extracts failure log elements from dictionary and writes them to a csv file

    Creates failure log using the file_name and error_message from the dictionary on
    the data variable. Writes CSV file in the directory within the failure_logs_directory
    variable, adds the header 'file_name,error_message', and adds the constructed
    failure log to the CSV file as the first entry.

    Args:
    failure_log_file: The fully qulified path to and name of the failure logs file.
                               'failure_logs_dir/failure_log.csv'
    data: A dict containing a file_name and a error message.
          {'file_name':'qwerty.sql','error_message':'stuff broke'}
    '''
    try:
        file_name = data['file_name']
        error_type = data['error_type']
        error_message = data['error_message']
        time_stamp = data['time_stamp']
        failure_log = [file_name,error_type,error_message,time_stamp]
    except KeyError as error:
        logger.info('''Unable to create valid failure logs with the given data.''')

    with open(failure_log_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['file_name','error_type','error_message','time_stamp'])
        writer.writerow(failure_log)

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

def get_latest_file(directory):
    '''

    '''
    files = os.listdir(directory)
    max_file = None
    max_number = -1

    for file in files:
        if file.endswith('.txt'):
            try:
                number = int(os.path.splitext(file)[0])
                if number > max_number:
                    max_number = number
                    max_file = file
            except ValueError:
                pass

    if max_file is not None:
        return os.path.join(directory, max_file)
    else:
        return None

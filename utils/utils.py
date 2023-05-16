from pathlib import Path
from google.api_core import exceptions as gcp_exceptions
import config
import datetime
import yaml
import csv
import os
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def format_failure_log_data(data: dict) -> list:
    ''' Create a failure log from a given dictionary

    Verifies the dictionary contains the required keys, adds them to a list, and return it.

    Args:
    data: A dict containing a the keys file_name, error_type, error message and timestamp.
          {'file_name':'path',
          'error_type':Exception,
          'error_message':'An error message',
          'timestamp':2023-05-12 15:47:25.067662}
    '''
    failure_log = []
    if isinstance(data, dict):
        logger.info(f'Creating failure log from provided data')
        try:
            failure_log = [data['file_name'],
                           data['error_type'],
                           data['error_message'],
                           data['timestamp']]
            logger.info(failure_log)
            return failure_log
        except KeyError as error:
            logger.error(f'"data" did not have the required keys to create failure log')
            return
    else:
        logger.error(f'Unable to construct failure log with given data: {data}')
        return

def create_failure_log(failure_log_path:Path,
                       data:dict):
    ''' Writes failure logs to a csv file in the directory specified

    Takes the provided dictionary, validates that it has the required data for a failure log,
    formats the failure log list as required, and writes the results and the proper headers
    to a file in the given location.

    Args:
    failure_log_file: Path to file for failure log
    data: A dict containing a the keys file_name, error_type, error message and timestamp.
          {'file_name':'path',
          'error_type':Exception,
          'error_message':'An error message',
          'timestamp':2023-05-12 15:47:25.067662}
    '''
    failure_log = format_failure_log_data(data)
    header = ['file_name','error_type','error_message','time_stamp']

    write_data_to_csv_file(file_path=failure_log_path,
                           header=header,
                           row=failure_log)

def write_data_to_csv_file(file_path:Path,
                           row:list,
                           header:list):
    ''' Takes a list of values and adds them to a csv file with the given header.

    The list should contain a nested list for each row, if only a single list is given,
    it is written to a single row in the csv file.

    Args:
    file_name: The name of a existing csv file to write the data to.
    rows: A list of elements representing a row to append to the given csv file.
          IE ['elementA','element1','elementB']
    '''
    file_path_string = str(file_path)
    with open (file_path_string, 'w', newline='') as csvfile:
        try:
            writer = csv.writer(csvfile)
            writer.writerow(header)
            writer.writerow(row)
        except Exception as error:
            message = f'Failed to create failure log for run.\n{error}'
            logger.error(message)

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
    ''' Returns the file in the given directory with the highest datetime

    Args:
    directory: Directory to evaluate.
    '''
    files = os.listdir(directory)
    max_file = None
    max_number = -1

    for file in files:
        if file.endswith('.csv'):
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

def create_path_if_not_exists(path) -> None:
    """
    Create the file path if it does not exist

    Args:
    path: the file path we are creating if it doesnt exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)





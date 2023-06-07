import logging
import re
import os
from google.cloud import bigquery

import config
from utils import utils, gcp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

business_unit_map = {
    "A": "dataset_a",
    "B": "dataset_b",
    "C": "dataset_c",
    "D": "dataset_d",
    "CREDIT": "dataset_e"
}


def generate_table_mapping(project_id: str,
                           dataset_id: str,
                           uc4_job_name: str,
                           business_unit: str):
    """
    Generates the teradata sql to bq sql dataset mapping so that each teradata sql gets put into the correct bq dataset.

    Args:
    project_id: the project_id used to run the functions needed.
    dataset_id: the dataset_id used to run the functions needed.
    uc4_job_name: The name of the specific uc4_job we are trying to get all the sql from.
    business_unit: the business unit that is associated with each bq dataset, this is how we know what teradata datasets = the specific bq datasets.
    """

    # Get the JSON
    uc4_job = utils.get_uc4_json(project_id=config.PROJECT,
                                 dataset_id=config.DATASET,
                                 uc4_job_name=uc4_job_name)

    dataset = business_unit_map[business_unit]
    repo_path = config.DATASET_MAPPING_OUTPUT

    # Find the interem tables
    sqls = utils.get_sql_dependencies(uc4_job=uc4_job,
                                      repo_path=repo_path)

    table_mapping_ddl = {}
    table_mapping_dml = {}
    object_mapping = {}
    object_list = []
    dml_string = ''
    ddl_string = ''

    # Identify if the SQLs are DML or DDL
    for sql in sqls:
        print(f'Attempting to locate the match ddl and dml statements in: {sql}')
        ddl_regex_pattern = '^CREATE|DROP|ALTER|TRUNCATE|RENAME'
        dml_regex_pattern = '^SELECT|INSERT|UPDATE|DELETE'
        sql_statement_list = re.split(';', sql)

        for sql_statement in sql_statement_list:
            ddl_match = re.match(ddl_regex_pattern, sql_statement)
            dml_match = re.match(dml_regex_pattern, sql_statement)
            if ddl_match:
                # Tables will look like dataset.table. We can split by period to get the dataset
                split_match = str(ddl_match.string).split('.')
                database = split_match[0].split(" ")
                print("database is ", database)
                table_id = split_match[2].split(" ")
                ddl_string = f'{ddl_string}{sql_statement};'
                print("split match is", split_match)
                table_mapping_ddl[
                    f"Teradata dataset is {ddl_match.string}"] = f"""BigQuery version is
                                                                     {database[-1]}.{dataset}.{table_id[0]}"""
                print("table mapping DDL is", table_mapping_ddl)
                table_id = table_id[0]
                mapping_block = {
                    "source": {
                        "type": "",
                        "database": "SIMBA",
                        "schema": f"{split_match[1]}",
                        "relation": f"{table_id}"},
                    "target": {
                        "database": "gcp_project",
                        "schema": f"{dataset}"}
                }
                object_list.append(mapping_block)

            if dml_match:
                # Tables will look like dataset.table. We can split by period to get the dataset
                table_mapping_dml[sql] = dml_match
                dml_string = f'{dml_string}{dml_match.string};'
                print(f"Found the following DML SQL Statements {sql_statement}")


    object_mapping["name_map"] = object_list
    print("object_mapping is ", object_mapping)
    os.system(f"echo {object_mapping} > object_mapping.config")

    # Write the table mappings to BigQuery
    # ddl_table_mapping = str(table_mapping_ddl)
    # dml_sql = str(table_mapping_dml)
    client = bigquery.Client()
    if ddl_string == '':
        ddl_string = 'Null'
    if dml_string == '':
        dml_string = 'Null'

    try:
        query = f"""
        INSERT INTO {config.PROJECT}.{dataset}.dataset_mapping
        (table_mapping_ddl, table_mapping_dml)
        VALUES('''{ddl_string}''', '''{dml_string}''')
        """
        print(f'\nSubmitting query: {query}\n')
        insert_query = client.query(query)
        results = insert_query.result()
        print(f"{results} uploaded to dataset_mapping table")

    except Exception as error:
        print(error)

    return table_mapping_ddl

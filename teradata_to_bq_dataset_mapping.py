from utils import utils, gcp
from google.cloud import bigquery
import config
import json
import re

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

business_unit_map = {
    "A": "dataset_a",
    "B": "dataset_b",
    "C": "dataset_c",
    "D": "dataset_d"
}

def generate_table_mapping(project_id:str,
                           dataset_id: str,
                           uc4_job_name:str,
                           business_unit: str):
    """
    Generates the teradata sql to bq sql dataset mapping so that each teradata sql gets put into the correct bq dataset."

    Args:
    project_id: the project_id used to run the functions needed.
    dataset_id: the dataset_id used to run the functions needed.
    uc4_job_name: The name of the specific uc4_job we are trying to get all the sql from.
    business_unit: the business unit that is associated with each bq dataset, this is how we know what teradata datasets = the specific bq datasets.
    """

    # Get the JSON
    uc4_job = utils.get_uc4_json(project_id=config.PROJECT, dataset_id=config.DATASET, uc4_job_name=uc4_job_name)

    dataset = business_unit_map[business_unit]

    # Find the interem tables
    sqls = utils.get_sql_dependencies(uc4_job=uc4_job,
                                      repo_path=config.E2E_OUTPUT)

    table_mapping = {}
    # Identify if the SQLs are DML or DDL
    for sql in sqls:
        match = re.search("^.", sql)

        if match is not None:
            # Tables will look like dataset.table. We can split by period to get the dataset
            split_match = str(match.string).split('.')
            print("split match is", split_match)
            table_mapping[split_match[1]] = f"{dataset}"
            print("table mapping is", table_mapping)

    table_mapping = str(table_mapping)

    # Write the table mappings to BigQuery
    client= bigquery.Client()

    try:
        insert_query = client.query(f"""
                                    INSERT INTO {config.PROJECT}.{dataset}.dataset_mapping (table_mapping)
                                    VALUES('''{table_mapping}''')
                                    """)
        results  = insert_query.result()
        print(f"{results} uploaded to dataset_mapping table")

    except Exception as error:
        print(error)

    return table_mapping

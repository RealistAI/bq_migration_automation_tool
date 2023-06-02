business_unit_map = {
    "A": "dataset_a",
    "B": "dataset_b",
    "C": "dataset_c",
    "D": "dataset_d"
}

def generate_table_mapping(project_id:str, dataset: str, uc4_job_name:str, business_unit: str):

    # Get the JSON
    uc4_job = utils.get_uc4_json()

    dataset = business_unit_map[business_unit]

    # Find the interem tables
    sqls = utils.get_sql_dependencies(uc4_job, config.REPO_PATH)

    table_mapping = {}
    # Identify if the SQLs are DML or DDL
    for sql in sqls:
       match = re.whatever(sql)

       if match is not None:
           # Tables will look like dataset.table. We can split by period to get the dataset
           split_match = match.split('.')

           table_mapping[match] = f"{dataset}.{split_match[1]}"

    # Write the table mappings to BigQuery

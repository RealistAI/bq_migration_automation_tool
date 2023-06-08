import config
import sort_queries
import teradata_to_bq_dataset_mapping


def main():
    list_of_uc4_jobs = sort_queries.sort_queries(config.PROJECT, config.DATASET)
    for uc4_job in list_of_uc4_jobs:
        job_name = uc4_job['uc4_job_name']
        business_unit = uc4_job['business_unit']
        teradata_to_bq_dataset_mapping.generate_table_mapping(project_id=config.PROJECT,
                                                              dataset_id=config.DATASET,
                                                              uc4_job_name=job_name,
                                                              business_unit=business_unit)


if __name__ == "__main__":
    main()

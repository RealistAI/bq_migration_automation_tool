import teradata_to_bq_dataset_mapping as tdm
import config

mapping = tdm.generate_table_mapping(config.PROJECT, config.DATASET, '"UC4_JOB_1"', "A")

print(mapping[0])
print("\n")
print(mapping[1])

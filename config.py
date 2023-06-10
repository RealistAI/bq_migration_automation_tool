from pathlib import Path
import os
import logging

##############################################################################
# Common Config                                                              #
#############################################################################
LOGGING_LEVEL = logging.INFO
# The CSV File containing a list of UC4 Jobs
UC4_CSV_FILE = Path(Path.cwd(), "uc4_jobs.csv")

# The GCP Project where the metadata will be stored
METADATA_PROJECT = 'michael-gilbert-dev'
METADATA_DATASET = 'uc4_conversion_metadata'

# The UC4 XML to JSON conversion stores the JSON in a BigQuery table. 
# This is its name
UC4_JSON_TABLE = f"{METADATA_PROJECT}.{METADATA_DATASET}.uc4_json"

##############################################################################
# Generate Teradata to BigQuery Mapping Config                               #
#############################################################################

# The table that stores the Teradata to BigQuery Mapping
TD_TO_BQ_MAPPING_TABLE = \
        f"{METADATA_PROJECT}.{METADATA_DATASET}.teradata_to_bigquery_mapping"

# A CSV file containing the mapping between business units and datasets
BUSINESS_UNIT_DATASET_MAP_CSV_FILE = Path(Path.cwd(), 
                                          'business_unit_dataset_map.csv')
# Repo containing the dwh-migration-tooling
DWH_MIGRATION_TOOL_REPO = {
        "path": "https://github.com/google/dwh-migration-tools.git",
        "branch": "main"
        }

UC4_SQL_REPO_NAME = 'UC4_SQL'

# Repo containing the SQLS to be translated.
UC4_SQL_REPO = {
        "path": f"https://github.com/RealistAI/{UC4_SQL_REPO_NAME}.git",
        "branch": "master"
        }

BASE_PATH = Path(Path.home(), 'required_repos')

# GCP Project that should perform the translations
PROJECT = "michael-gilbert-dev"

# GCS URI For the preprocessed blobs
PREPROCESED_BUCKET = "gs://dwh_preprocessed"

# Directory housing the GitHub repository with the Teradata SQL to translate
SOURCE_SQL_PATH = Path(BASE_PATH, UC4_SQL_REPO_NAME, 'teradata_sql')

# GCS URI For the translated blobs
TRANSLATED_BUCKET = "gs://dwh_translated"

# Path to local directory containing the transpiled SQL
SQL_TO_VALIDATE = Path(os.getcwd(), "transpiled_sql")

# The directory in the origin GitHub repo for the validated Google SQL
TARGET_SQL_PATH = Path(BASE_PATH, UC4_SQL_REPO_NAME, 'bigquery_sql')

# Path to the DWH Migration tool required config.
CONFIG_BASE = Path(os.getcwd(), 'config')
CONFIG_YAML = Path(CONFIG_BASE, 'config.yaml')
OBJECT_MAPPING = Path(CONFIG_BASE, "name_map.json")

# Debug mode?
DEBUG = False

DATASET = "UC4_Jobs"

LOGGING = f'`{PROJECT}.{DATASET}.transpilation_logs`'

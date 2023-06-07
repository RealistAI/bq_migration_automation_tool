from pathlib import Path
import os


# Repo containing the dwh-migration-tooling
DWH_MIGRATION_TOOL_REPO = {
        "path": "https://github.com/google/dwh-migration-tools.git",
        "branch": "main"
        }

UC4_SQL_REPO_NAME = 'UC4_SQL'

TEST_DATASET_MAPPING_PATH = "~/git/UC4_SQL"

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

DATASET_MAPPING_OUTPUT = Path(BASE_PATH, UC4_SQL_REPO_NAME, "output", "bteq", "BU", "SIMBA", "AMPS")

E2E_OUTPUT = Path(os.getcwd(), "output", "bteq", "BU", "SIMBA", "AMPS")

# Path to local directory containing the transpiled SQL
SQL_TO_VALIDATE = Path(os.getcwd(), "transpiled_sql")

# The directory in the origin GitHub repo for the validated Google SQL
TARGET_SQL_PATH = Path(BASE_PATH, UC4_SQL_REPO_NAME, 'bigquery_sql')

# Path to the DWH Migration tool required config.
CONFIG = Path(os.getcwd(), 'config', 'config.yaml')

OBJECT_MAPPING = Path(os.getcwd(), 'config', "object_name_mapping.json")

# Path to object name mapping config file
MAPPING_CONFIG_FILE = Path(os.getcwd(), 'config', 'name_mapping.json')

# Debug mode?
DEBUG = True

DATASET = "UC4_Jobs"

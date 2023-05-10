from pathlib import Path
import os

DWH_MIGRATION_TOOL_REPO = {
        "path": "https://github.com/google/dwh-migration-tools.git",
        "branch": "main"
        }
UC4_SQL_REPO = {
        "path": "https://github.com/RealistAI/UC4_SQL.git",
        "branch": "master"
        }

BASE_PATH = Path(Path.home(), 'required_repos')

# BQ Migration Service Config
PROJECT = "michael-gilbert-dev"
PREPROCESED_BUCKET = "gs://dwh_preprocessed"
SOURCE_SQL_PATH = Path(BASE_PATH, 'UC4_SQL', 'teradata_sql')
TRANSLATED_BUCKET = "gs://dwh_translated"
TARGET_SQL_PATH = Path(BASE_PATH, 'UC4_SQL', 'bigquery_sql')
CONFIG = Path(os.getcwd(), 'config', 'config.yaml')
MAPPING_CONFIG_FILE = Path(os.getcwd(), 'config', 'name_mapping.json')
DEBUG = True

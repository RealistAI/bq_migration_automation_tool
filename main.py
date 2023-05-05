import os
from google.cloud import bigquery
from google.cloud import storage
import yaml


# Transpilation
# Validate config file
with open('config/config.yaml') as file:
    config = yaml.load(f, Loader=yaml.FullLoader)

project = config['gcp_project']
bucket = config['gcs_bucket']


# Trigger run.sh


# Dryrun
# Parse through output and dryrun all transpiled SQL


# Git integration
# Create branch
# Commit and start PR

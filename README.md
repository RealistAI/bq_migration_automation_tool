# BigQuery Migration Automation Tool
The BigQuery Migration Automation Tool is used with the BigQuery Migration Service to batch transpile all of the teradata SQLs to BigQuery SQLs and then validates those queries with Bigquery dry-run. Then the BigQuery SQLs are pushed to github as well as a transpilation logs table in BigQuery. <br>

## Architecture 

![bq-migration-tool-flow drawio (1)](https://github.com/RealistAI/bq_migration_automation_tool/assets/99982739/44e9167f-e987-4a25-ba95-fde4cc78ff41)

## Tools Used
* BQ Migration CLI - CLI tooling built by the BigQuery Migration Service team that allows you to interact with the BigQuery Migration Service.
* Makefile- A Makefile is used to run the python scripts, the pip installs and all the requirements needed to run the BigQuery Migration Automation Tool. <br><br>

## Required repositories 
UC4 SQL Repo\
This is the repository of all of the validated SQLs generated by the BigQuery Migration 
Service tool. The tool uploads the verified SQL to this repository.

Folder Structure:
<pre>
|-- Teradata SQLs
    |--Sub Folders
        |-- my_sql.sql
        … 
|-- BigQuery SQLs
    |--Sub Folders
        |-- my_sql.sql
</pre>

The Teradata SQLs folder contains all of the Teradata SQLs for the UC4 Jobs.
The BigQuery SQLs folder contains all of the converted BigQuery SQLs for the UC4 Jobs

DWH Migration Tools Repo\
This is the Github repository that contains the dwh-migration-tools that is required 
for the transpilation of the Teradata SQL. <br>

## Setup 
The first part of the Makefile will run the setup.py. This script will clone the required repos 
into the local file system, if the given Github repo exists already in our local file system, we will 
do a git pull instead. <br>

## Dataset Mapping
The Dataset Mapping parses through all the SQL's in the `/teradata_sql` to find their dataset and project, 
and then maps them to their proper Bigquery dataset and project for the transpilation process. <br>

## Transpilation
The transpilation is completed using `bqms-run`. The script sets the environment variables required by 
the BQMS tool and then run the `bqms-run` command to initilize the transpilation process. <br>

## Dry Run
We then iterate through the files in the BQMS output submit a dry run query for each of them.
If the query is successful the file will then be moved into the UC4_SQL_REPO in the bigquery_sql/ 
directory.<br>

## Transpilation Logs
At the end of the Dry-run validation, whether a dry-run is successul for not, the query data is uploaded to the transpilation_logs table in BigQuery where it can be accessed to get accurate logs for the dry-run success or failure. If the Dry-run is successful it will have a status of `SUCCEEDED`, it will have the time the dry-run ran and the specific query that succeeded. If the dry-run fails it will have a status of `FAILED`, it will have the time the dry-run ran, the specific query that failed and the error message explaining why the dry-run validation wasn't successful. <br>

## Github Integration
Upon completion of the validaiton process, the script will create a new branch in the origin repository, 
and push the updated UC4 SQL. <br>

## Usage
In order to utilize this tool, you first need to clone the project into the directory of your choice 
`git clone https://github.com/RealistAI/bq_migration_automation_tool.git`, navigate into the newly cloned 
directory `cd bq_migration_automation_tool`, and alter the config.py to your own specification. Create 
a Pip virtual environment using `pipenv shell` and install the required libraries `pipenv install`, 
and run the Makefile `make run`. <br>

## Configuration Options

#### DWH_MIGRATION_TOOL_REPO

The repository url and branch containing the dwh-migration-tools<br>


#### UC4_SQL_REPO

The repository url and branch containing the SQL's to transpile & validate<br>


#### UC4_SQL_REPO_NAME

The name of the repository containing the SQL's to transpile & validate<br>


#### BASE_PATH

The base path for which the dataset mapping grabs the SQLs that is parses through and adjusts to work for BigQuery<br>


#### PROJECT

The name of the Google Cloud Platform project that will perform the bulk transpilation & validation<br>


#### DATASET

The name of the Google Cloud Platform Dataset that will perform the bulk transpilation & validation.<br>


#### PREPROCESED_BUCKET

A Google Cloud Storage bucket that will be used by `bqms-run` as a staging area for the translation process<br>


#### SOURCE_SQL_PATH

The directory in your Github repository containing .sql files for translation and validation<br>


#### TRANSLATED_BUCKET

A Google Cloud Storage bucket that will be used by `bqms-run` to store translated files before dumping 
them back into the local file system.<br>


#### SQL_TO_VALIDATE

The local directory that `bqms-run` will use to store the results of the run.<br>


#### TARGET_SQL_PATH

The directory within the origin Github repository to contain the translated and validated .sql files<br>


#### CONFIG_BASE

The path to the base config directyory which hosts the config.yaml file and the object name mapping file.<br>


#### CONFIG_YAML 

The path to the dwh-migration-tools config.yaml file.<br>


#### OBJECT MAPPING

The path to the object name mapping configuration file.<br>

# BigQuery Migration Automation Tool

The BigQuery Migration Automation Tool is used to batch transpile all of the Simba SQLs and push those transpiled SQLs to github.

## Tools Used
BQ Migration CLI - CLI tooling built by the BigQuery Migration Service team that allows you to interact with the BigQuery Migration Service.

Makefile- A Makefile is used to run the python scripts, the pip installs and all the requirements needed to run the BigQuery Migration Automation Tool.

## Workflow
UC4 SQL Repo
This is the repository of all of the validated SQLs generated by the BigQuery Migration Service tool. The tool uploads the verified SQL to this repository.

Folder Structure:
|-- Teradata SQLs
    |--Sub Folders
        |-- my_sql.sql
        … 
|-- BigQuery SQLs
    |--Sub Folders
        |-- my_sql.sql

The Teradata SQLs folder contains all of the Teradata SQLs for the UC4 Jobs.
The BigQuery SQLs folder contains all of the converted BigQuery SQLs for the uc4 Jobs

## Transpilation
The first step in the tool is to convert the Teradata SQLs to BigQuery SQLs. It does the work in this order:

Get the latest version of the UC4 SQL Repo.
If it is not on the local file system, run `git clone`.
If it is, run `git pull`.
Call the BigQuery Migration Service, passing in all of the files contained in the `Teradata SQLs` folder of the UC4 SQL Repo.
Output all of the SQLs to the output path specified in the config file. 

## Dry Run
The next step is for the tool to perform a dry-run of each of the SQLs that have been converted. The dry-run ensures that the SQLs are syntactically correct.

For each of the SQLs in the output path, the tool does a dry-run to validate the SQL.
For the SQLs that have bind variables, the tool substitutes those values using a predefined mapping. E.G

SELECT * FROM ${project}.${dataset}.table;

If any of the dry-run fails:
The tool saves the bad SQl file to the `failure_logs` folder specified in the config.py file.
The tool adds a record to the `{current_datetime}.csv` file that will contain the file path to the invalid SQL and the error message returned by BigQuery. E.G
/invalid_sql/my_sql.sql, BigQuery Error Message.

If the dry-run succeeds, the tool copies the converted sql to the `validated_sql` folder specified in the config.py file.

## Git Integration
If there are any new files in the ‘validated_sql’ folder the tool does the following:

Creates a new git branch in the UC4 SQL Repo named ‘bq_migration_tool_batch_{current_datetime}’.
Copies the SQL files from the ‘validated_sql’ folder to the UC4 SQL Repo. 
Commits changes and push those changes to Git.
Creates a Pull Request to the main branch of the UC4 SQL Repo.

## Usage
To run the tool, the user will run the following command:
Make

The config file will contain default values for:
source_file_path
target_file_path
git_repo 



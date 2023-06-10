run:
	pip install -r requirements.txt
	python setup.py
	pip install ~/required_repos/dwh-migration-tools/client
	python upload_teradata_to_bigquery_mapping.py
	python translate_sql.py

generate-mapping:
	python generate_td_to_bq_mapping.py

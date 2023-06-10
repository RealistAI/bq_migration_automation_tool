install:
	pip install -r requirements.txt

run:
	python setup.py
	pip install dwh-migration-tools/client
	python upload_teradata_to_bigquery_mapping.py
	python translate_sql.py

generate-mapping:
	python generate_td_to_bq_mapping.py

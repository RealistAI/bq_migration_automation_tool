gcloud storage rm -a gs://dwh_translated/**;
gcloud storage rm -a gs://dwh_preprocessed/**;
rm -rf source_sql/*;
rm -rf transpiled_sql/*;


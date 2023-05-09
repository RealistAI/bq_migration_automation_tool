gcloud storage rm -a gs://dwh_preprocessed/**;
gcloud storage rm -a gs://dwh_translated/**;
rm -rf invalid_sql/*;
rm -rf validated_sql/*;

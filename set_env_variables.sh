#!/bin/bash
export BQMS_PROJECT='michael-gilbert-dev';
export BQMS_PREPROCESSED_PATH='gs://dwh_preprocessed';
export BQMS_INPUT_PATH='source_sql/';
export BQMS_TRANSLATED_PATH='gs://dwh_translated';
export BQMS_POSTPROCESSED_PATH='transpiled_sql/';
export BQMS_CONFIG_PATH='config/config.yaml';

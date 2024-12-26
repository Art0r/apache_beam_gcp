#!/bin/sh

rm -rf ./venv/

python3 -m venv venv

. ./venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e .

python main.py \
  --setup_file="./setup.py" \
  --runner="DataflowRunner" \
  --project="curso-apache-beam-gcp" \
  --region="us-central1" \
  --temp_location="gs://curso-apache-beam-gcp-n/temp" \
  --staging_location="gs://curso-apache-beam-gcp-n/staging" \
  --template_location="gs://curso-apache-beam-gcp-n/template/postgres-mongodb-to-bq" \
  --save_main_session


gcloud dataflow jobs run postgres-mongodb-to-bq-job \
  --gcs-location gs://curso-apache-beam-gcp-n/template/postgres-mongodb-to-bq \
  --region us-central1 \
  --staging-location gs://curso-apache-beam-gcp-n/staging

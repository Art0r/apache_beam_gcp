#!/bin/sh

. venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e .

python main.py \
  --requirements_file="./requirements.txt" \
  --setup_file="./setup.py" \
  --runner="DataflowRunner" \
  --project="curso-apache-beam-gcp" \
  --region="us-central1" \
  --temp_location="gs://curso-apache-beam-gcp-n/temp" \
  --staging_location="gs://curso-apache-beam-gcp-n/staging" \
  --template_location="gs://curso-apache-beam-gcp-n/template/curso-apache-beam-postgres-bq-local-teste" \
  --pipeline="postgres"

gcloud dataflow jobs run curso-apache-beam-gcp-job-qwer121231124 \
  --gcs-location gs://curso-apache-beam-gcp-n/template/curso-apache-beam-postgres-bq-local-teste \
  --region us-central1 \
  --staging-location gs://curso-apache-beam-gcp-n/staging
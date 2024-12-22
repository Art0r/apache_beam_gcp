#!/bin/sh

. venv/bin/activate

python -m pip install -e .

python main.py --runner DataflowRunner --requirements_file ./requirements.txt --setup_file ./setup.py

# gcloud dataflow jobs run curso-apache-beam-gcp-job-qwer121231124 \
#   --gcs-location gs://curso-apache-beam-gcp-n/template/curso-apache-beam-postgres-bq-local-teste \
#   --region us-central1 \
#   --staging-location gs://curso-apache-beam-gcp-n/temp
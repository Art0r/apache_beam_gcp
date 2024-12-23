# Activate virtual environment
.\venv\Scripts\activate

# Upgrade pip and install the package
python -m pip install --upgrade pip
python -m pip install -e .

# Choosing pipeline type (mongodb or postgres)
$pipelineChoice = Read-Host "Choose a pipeline (mongodb/postgres)"

if ($pipelineChoice -eq "mongodb") {

    python main.py `
        --requirements_file="./requirements.txt" `
        --setup_file="./setup.py" `
        --runner="DataflowRunner" `
        --project="curso-apache-beam-gcp" `
        --region="us-central1" `
        --temp_location="gs://curso-apache-beam-gcp-n/temp" `
        --staging_location="gs://curso-apache-beam-gcp-n/staging" `
        --template_location="gs://curso-apache-beam-gcp-n/template/curso-apache-beam-mongo-bq" `
        --pipeline="mongodb"

} elseif ($pipelineChoice -eq "postgres") {

  python main.py `
        --requirements_file="./requirements.txt" `
        --setup_file="./setup.py" `
        --runner="DataflowRunner" `
        --project="curso-apache-beam-gcp" `
        --region="us-central1" `
        --temp_location="gs://curso-apache-beam-gcp-n/temp" `
        --staging_location="gs://curso-apache-beam-gcp-n/staging" `
        --template_location="gs://curso-apache-beam-gcp-n/template/curso-apache-beam-postgres-bq" `
        --pipeline="postgres"

} else {

    Write-Host "Invalid choice. Please select 'mongodb' or 'postgres'."

}


# Optional: Uncomment to run the Dataflow job using gcloud
# gcloud dataflow jobs run curso-apache-beam-gcp-job-qwer121231124 `
#   --gcs-location gs://curso-apache-beam-gcp-n/template/curso-apache-beam-postgres-bq-local-teste `
#   --region us-central1 `
#   --staging-location gs://curso-apache-beam-gcp-n/staging

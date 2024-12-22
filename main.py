"""
Dataflow Job/Pipeline to retrieve data from Posgresql and transform then upload to BigQuery
"""
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from curso_apache_beam_gcp_ext_lib.beam_custom_classes import ReadUsersFromPostgres, FormatUsersToBq
from curso_apache_beam_gcp_ext_lib.env_vars import assign_pair_key_value_to_env, access_secret_version


CLOUD_STORAGE = "gs://curso-apache-beam-gcp-n"
CLOUD_STORAGE_TEMP = f"{CLOUD_STORAGE}/temp"
CLOUD_STORAGE_TEMPLATE_FILE = f"{CLOUD_STORAGE}/template/curso-apache-beam-postgres-bq-local-teste"

BIG_QUERY_TABLE = "curso-apache-beam-gcp:cursoapachebeamgcpdataset.users"

SERVICE_ACCOUNT = "curso-apache-beam-gcp.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT


for key_pair in access_secret_version(project_id="curso-apache-beam-gcp",
                                      secret_id="psql-bq-dataflow").split('\n'):

    assign_pair_key_value_to_env(key_value=key_pair)


posgres_db_config = {
    "host": os.environ.get('PSQL_HOST'),
    "port": os.environ.get('PSQL_PORT', 5432),
    "database": os.environ.get('PSQL_NAME'),
    "user": os.environ.get('PSQL_USER'),
    "password": os.environ.get('PSQL_PASSWORD')
}


pipelines_options = PipelineOptions.from_dictionary({
    'project': 'curso-apache-beam-gcp',
    'runner': 'DataflowRunner',
    'region': 'us',
    'staging_location': CLOUD_STORAGE_TEMP,
    'temp_location': CLOUD_STORAGE_TEMP,
    'template_location': CLOUD_STORAGE_TEMPLATE_FILE,
    # 'save_main_session': True,
})

with beam.Pipeline(
    options=pipelines_options
) as p:
    postgres_rows = (
        p
        | "Create Input" >> beam.Create([None])
        | "Lendo Bando de Dados" >> beam.ParDo(ReadUsersFromPostgres(db_config=posgres_db_config))
        | "Formatando dados para o Big Query" >> beam.ParDo(FormatUsersToBq())
        | 'Enviando para o Big Query' >> beam.io.WriteToBigQuery(
            table=BIG_QUERY_TABLE,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=CLOUD_STORAGE_TEMP)
        # | "Lendo" >> beam.Map(print)
    )

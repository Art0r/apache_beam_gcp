import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from curso_apache_beam_gcp_ext_lib.beam_custom_classes import ReadUsersFromPostgres, FormatUsersToBq
from curso_apache_beam_gcp_ext_lib.env_vars import assign_pair_key_value_to_env, access_secret_version

def init_env_vars() -> None:
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "curso-apache-beam-gcp.json"

    for key_pair in access_secret_version(project_id="curso-apache-beam-gcp",
                                          secret_id="psql-bq-dataflow").split('\n'):

        assign_pair_key_value_to_env(key_value=key_pair)


def run_pipeline(pipeline_args) -> None:
    pipeline_options = PipelineOptions(pipeline_args)

    init_env_vars()

    with beam.Pipeline(
            options=pipeline_options
    ) as p:
        rows = (
            p
            | "Create Input" >> beam.Create([None])
            | "Lendo Bando de Dados" >> beam.ParDo(ReadUsersFromPostgres())
            | "Formatando dados para o Big Query" >> beam.ParDo(FormatUsersToBq())
            # | 'Enviando para o Big Query' >> beam.io.WriteToBigQuery(
            #     table="curso-apache-beam-gcp:cursoapachebeamgcpdataset.users",
            #     schema="SCHEMA_AUTODETECT",
            #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            | "Lendo" >> beam.Map(print)
        )

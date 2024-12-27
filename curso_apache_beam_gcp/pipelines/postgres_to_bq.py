import logging
import os
import apache_beam as beam
from curso_apache_beam_gcp.beam_custom_classes import FormatUsersFromMongoDbToBq, ReadUsersFromPostgres, FormatUsersFromPostgresToBq
from curso_apache_beam_gcp.env_vars import find_value_for_keys
from apache_beam.io.gcp.internal.clients.bigquery import TableReference


def run_pipeline(p: beam.Pipeline,
                 is_from_direct_runner: bool,
                 logger: logging.Logger):
    # Postgres Piopeline
    logger.info("Extracting and Transforming Postgres data")
    postgres_result = (
        p
        | "Create Input" >> beam.Create([None])
        | "Lendo Bando de Dados (Postgresql)" >> beam.ParDo(
            ReadUsersFromPostgres()
        )
        | "Formatando dados para o Big Query (Postgresql)" >> beam.ParDo(
            FormatUsersFromPostgresToBq()
        )
    )
    logger.info("Finished Extracting and Transforming Postgres data")

    # If is direct runner it must print the elements instead of sending them to BQ
    if is_from_direct_runner:

        logger.info("Loading Postgresql data into BigQuery")
        postgres_result | "Lendo (Postgresql)" >> beam.Map(
            lambda element: print("Lendo (Postgresql): ", element))

    else:

        logger.info("Loading Postgresql data into BigQuery")

        postgres_result | 'Enviando para o Big Query (Postgresql)' >> beam.io.WriteToBigQuery(
            table=os.environ.get('GCP_BIGQUERY_TABLES'),
            schema="SCHEMA_AUTODETECT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        logger.info("Finished Loading Postgresql data into BigQuery")

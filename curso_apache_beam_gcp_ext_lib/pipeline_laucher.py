import os
import sys
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from curso_apache_beam_gcp_ext_lib.beam_custom_classes import (ReadUsersFromPostgres,
                                                               FormatUsersFromPostgresToBq,
                                                               FormatUsersFromMongoDbToBq)
from curso_apache_beam_gcp_ext_lib.env_vars import assign_pair_key_value_to_env, access_secret_version
from apache_beam.io.gcp.internal.clients.bigquery import TableReference


def init_env_vars() -> None:

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "curso-apache-beam-gcp.json"

    # for every KEY=value it will be created a env var as os.environ[KEY] = value
    for key_pair in access_secret_version(project_id="curso-apache-beam-gcp",
                                          secret_id="psql-bq-dataflow").split('\n'):

        assign_pair_key_value_to_env(key_value=key_pair)


def run_pipeline(pipeline_args: list[str], logger: logging.Logger) -> None:

    logger.info("Getting environment variables")
    init_env_vars()

    is_from_direct_runner = '--runner=DirectRunner' in pipeline_args

    with beam.Pipeline(
        options=PipelineOptions(pipeline_args)
    ) as p:

        # MongoDB Pipeline
        user = os.environ.get("MONGO_USER")
        password = os.environ.get("MONGO_PASSWORD")
        db = os.environ.get("MONGO_DB")

        logger.info("Extracting and Transforming MongoDB data")
        mongodb_result = (
            p
            | "Lendo Bando de Dados (Mongodb)" >> beam.io.ReadFromMongoDB(
                uri=f"mongodb+srv://{user}:{password}@cluster0.gsxb1.mongodb.net",
                db="sample_mflix",
                coll="users",
                bucket_auto=True
            )
            | "Formatando dados para o Big Query (MongoDB)" >> beam.ParDo(FormatUsersFromMongoDbToBq())
        )
        logger.info("Finished Extracting and Transforming MongoDB data")

        # If is direct runner it must print the elements instead of sending them to BQ
        if is_from_direct_runner:

            logger.info("Loading MongoDB data into BigQuery")
            mongodb_result | "Lendo (MongoDb)" >> beam.Map(
                lambda element: print("Lendo (MongoDb): ", element))

        else:

            logger.info("Loading MongoDB data into BigQuery")
            mongodb_result | 'Enviando para o Big Query (MongoDB)' >> beam.io.WriteToBigQuery(
                table="curso-apache-beam-gcp.cursoapachebeamgcpdataset.users",
                schema="SCHEMA_AUTODETECT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            logger.info("Finished loading MongoDB data into BigQuery")

        # Postgres Pipeline
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
            table_ref = TableReference(
                datasetId="cursoapachebeamgcpdataset",
                projectId="curso-apache-beam-gcp",
                tableId="users"
            )

            postgres_result | 'Enviando para o Big Query (Postgresql)' >> beam.io.WriteToBigQuery(
                table=table_ref,
                schema="SCHEMA_AUTODETECT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

            logger.info("Finished Loading Postgresql data into BigQuery")

import logging
import os
import apache_beam as beam
from curso_apache_beam_gcp.beam_custom_classes import FormatUsersFromMongoDbToBq
from curso_apache_beam_gcp.env_vars import find_value_for_keys


def run_pipeline(p: beam.Pipeline,
                 is_from_direct_runner: bool,
                 logger: logging.Logger):
    # MongoDB Pipeline
    secrets = find_value_for_keys(
        "MONGO_USER", "MONGO_PASSWORD", "MONGO_DB")

    user = secrets.get('MONGO_USER')
    password = secrets.get('MONGO_PASSWORD')
    db = secrets.get('MONGO_DB')

    logger.info("Extracting and Transforming MongoDB data")
    mongodb_result = (
        p
        | "Lendo Bando de Dados (Mongodb)" >> beam.io.ReadFromMongoDB(
            uri=f"mongodb+srv://{user}:{password}@cluster0.gsxb1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
            db=db,
            coll="users",
            bucket_auto=True,
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
            table=os.environ.get('GCP_BIGQUERY_TABLES'),
            schema="SCHEMA_AUTODETECT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        logger.info("Finished loading MongoDB data into BigQuery")

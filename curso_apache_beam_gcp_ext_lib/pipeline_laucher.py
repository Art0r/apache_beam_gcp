import os
import sys
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


def run_pipelines(pipeline_args) -> None:

    pipeline_options = PipelineOptions(pipeline_args)

    init_env_vars()

    is_from_direct_runner = '--runner=DirectRunner' in pipeline_args

    is_mongo_pipeline = '--pipeline=mongodb' in pipeline_args
    is_postgres_pipeline = '--pipeline=postgres' in pipeline_args
    
    if is_mongo_pipeline:
        return run_mongo_db_pipeline(pipeline_options, is_from_direct_runner)
        
    if is_postgres_pipeline:
        return run_postgres_pipeline(pipeline_options, is_from_direct_runner)
        
    

def run_mongo_db_pipeline(pipeline_options: PipelineOptions, is_from_direct_runner: bool):
    

    # MongoDB Pipeline
    with beam.Pipeline(
                options=pipeline_options
        ) as p:

        user = os.environ.get("MONGO_USER")
        password = os.environ.get("MONGO_PASSWORD")
        db = os.environ.get("MONGO_DB")

        rows = (
            p
            | "Lendo Bando de Dados (Mongodb)" >> beam.io.ReadFromMongoDB(
                uri=f"mongodb+srv://{user}:{password}@cluster0.gsxb1.mongodb.net",
                db="sample_mflix",
                coll="users",
                bucket_auto=True
            )
            | "Formatando dados para o Big Query (MongoDB)" >> beam.ParDo(FormatUsersFromMongoDbToBq())       
        )

        # If is direct runner it must print the elements instead of sending them to BQ
        if is_from_direct_runner:
            
            rows | "Lendo (MongoDb)" >> beam.Map(lambda element: print("Lendo (MongoDb): ", element))

        else:

            rows | 'Enviando para o Big Query (MongoDB)' >> beam.io.WriteToBigQuery(
                    table="curso-apache-beam-gcp.cursoapachebeamgcpdataset.users",
                    schema="SCHEMA_AUTODETECT",
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            
            

def run_postgres_pipeline(pipeline_options: PipelineOptions, is_from_direct_runner: bool):
    
    # Postgresql Pipeline
    with beam.Pipeline(
            options=pipeline_options
    ) as p:
        rows = (
            p
            | "Create Input" >> beam.Create([None])
            | "Lendo Bando de Dados (Postgresql)" >> beam.ParDo(ReadUsersFromPostgres())
            | "Formatando dados para o Big Query (MongoDB)" >> beam.ParDo(FormatUsersFromPostgresToBq())
        )

        # If is direct runner it must print the elements instead of sending them to BQ
        if is_from_direct_runner:
            
            rows | "Lendo (Postgresql)" >> beam.Map(lambda element: print("Lendo (Postgresql): ", element))
        
        else:

            table_ref = TableReference(
                datasetId="cursoapachebeamgcpdataset",
                projectId="curso-apache-beam-gcp",
                tableId="users"
            )
        
            rows | 'Enviando para o Big Query (Postgres)' >> beam.io.WriteToBigQuery(
                    table=table_ref,
                    schema="SCHEMA_AUTODETECT",
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

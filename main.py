import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

from pyarrow.dataset import dataset

SERVICE_ACCOUNT = "curso-apache-beam-gcp.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT

CLOUD_STORAGE = "gs://curso-apache-beam-gcp-n"
CLOUD_STORAGE_INPUT = "gs://curso-apache-beam-gcp-n/inputs/voos_sample.csv"
CLOUD_STORAGE_TEMP = f"{CLOUD_STORAGE}/temp"
CLOUD_STORAGE_TEMPLATE_FILE = f"{CLOUD_STORAGE}/template/curso-apache-beam-bckt-bq-local-teste"

BIG_QUERY_TABLE = f"curso-apache-beam-gcp:cursoapachebeamgcpdataset.cursoapachebeamgcpdataset-flights"

pipelines_options = PipelineOptions.from_dictionary({
    'project': 'curso-apache-beam-gcp',
    'runner': 'DataflowRunner',
    'region': 'us',
    'staging_location': CLOUD_STORAGE_TEMP,
    'temp_location': CLOUD_STORAGE_TEMP,
    'template_location': CLOUD_STORAGE_TEMPLATE_FILE,
    'save_main_session': True
})

p = beam.Pipeline(options=pipelines_options)


# class FilterWithoutDelays(beam.DoFn):
# def setup(self):
#     super().setup()
#
# def start_bundle(self):
#     super().start_bundle()
#
# def finish_bundle(self):
#     super().finish_bundle()
#
# def teardown(self):
#     super().teardown()
#
# def process_batch(self, batch, *args, **kwargs):
#     super().process_batch(batch, *args, **kwargs)
#
# def process(self, element, *args, **kwargs):
#     try:
#         return int(element[8]) > 0
#     except (ValueError, IndexError):
#         return False


def split_by_commas(record):
    return record.split(',')


def filter_with_delays(record):
    try:
        return int(record[8]) > 0
    except (ValueError, IndexError):
        return False


def create_key_value(record):
    return record[4], int(record[8])


def format_for_bigquery(elem):
    print("before", elem)
    obj = {
        'Aeroporto': str(elem[0]),
        'SomaAtrasos': sum(elem[1]['soma_atrasos']),
        'ContagemAtrasos': sum(elem[1]['contagem_atrasos']),
    }
    print("after", obj)
    return obj


def process_data(pipeline: beam.Pipeline, type_of_pipeline: str):
    return (
            pipeline
            | f"Importar Dados ({type_of_pipeline})" >> beam.io.ReadFromText(CLOUD_STORAGE_INPUT, skip_header_lines=1)
            | f"Separar por Virgulas ({type_of_pipeline})" >> beam.Map(split_by_commas)
            | f"Voos sem atraso ({type_of_pipeline})" >> beam.Filter(filter_with_delays)
            | f"Criar par ({type_of_pipeline})" >> beam.Map(create_key_value)
    )


# GROUP BY + SUM
soma_atrasos = (
        process_data(p, "Soma Atrasos")
        | "Somar por key" >> beam.CombinePerKey(sum)
)

# GROUP BY + COUNT
contagem_atrasos = (
        process_data(p, "Contagem Atrasos")
        | "Contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
        {'contagem_atrasos': contagem_atrasos, 'soma_atrasos': soma_atrasos}
        | 'Agrupando por chave' >> beam.CoGroupByKey()
        | 'Transformar para formato BigQuery' >> beam.Map(format_for_bigquery)
        | 'Enviando para o Big Query' >> beam.io.WriteToBigQuery(
    table=BIG_QUERY_TABLE,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    custom_gcs_temp_location=CLOUD_STORAGE_TEMP)
)

p.run()

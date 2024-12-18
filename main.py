import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os

SERVICE_ACCOUNT = "curso-apache-beam-gcp.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT
CLOUD_STORAGE = "gs://curso-apache-beam-gcp-n"
CLOUD_STORAGE_OUTPUTS = CLOUD_STORAGE + '/outputs'


pipelines_options = PipelineOptions.from_dictionary({
    'project': 'curso-apache-beam-gcp',
    'runner': 'DataflowRunner',
    'region': 'multi-region',
    'staging_location': f'{CLOUD_STORAGE}/temp',
    'temp_location': f'{CLOUD_STORAGE}/temp',
    'template_location': f'{CLOUD_STORAGE}/template/curso-apache-beam-local-teste'
})

p = beam.Pipeline(options=pipelines_options)


class FilterWithoutDelays(beam.DoFn):
    def process(self, element, *args, **kwargs):
        if int(element[8]) > 0:
            return [element]


def process_data(pipeline: beam.Pipeline, type_of_pipeline: str):
    return (
        pipeline
        | f"Importar Dados ({type_of_pipeline})" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines=1)
        | f"Separar por Virgulas ({type_of_pipeline})" >> beam.Map(lambda record: record.split(','))
        | f"Voos sem atraso ({type_of_pipeline})" >> beam.ParDo(FilterWithoutDelays())
        | f"Criar par ({type_of_pipeline})" >> beam.Map(lambda record: (record[4], int(record[8])))
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
    | 'Group By' >> beam.CoGroupByKey()
    | 'Enviando para o Cloud Storage' >> beam.io.WriteToText(CLOUD_STORAGE_OUTPUTS + "/meu_arquivo.txt")
)

p.run()

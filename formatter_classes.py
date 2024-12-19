import apache_beam as beam

class SplitByCommasDoFn(beam.DoFn):
    def process(self, record):
        yield record.split(',')


# DoFn to filter records with delays
class FilterWithDelaysDoFn(beam.DoFn):
    def process(self, record):
        try:
            if int(record[8]) > 0:
                yield record
        except (ValueError, IndexError):
            pass


# DoFn to create key-value pairs
class CreateKeyValueDoFn(beam.DoFn):
    def process(self, record):
        yield record[4], int(record[8])


# DoFn to format records for BigQuery
class FormatForBigQueryDoFn(beam.DoFn):
    def process(self, element):
        key, grouped_data = element
        yield {
            'Aeroporto': key,
            'SomaAtrasos': sum(grouped_data['soma_atrasos']),
            'ContagemAtrasos': sum(grouped_data['contagem_atrasos']),
        }

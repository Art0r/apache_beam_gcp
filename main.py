"""
Dataflow Job/Pipeline to retrieve data from Posgresql and transform then upload to BigQuery
"""
from curso_apache_beam_gcp_ext_lib.pipeline_laucher import run_pipeline
import logging

logger = logging.getLogger()

if __name__ == "__main__":
    import sys
    pipeline_args = sys.argv[1:]
    run_pipeline(pipeline_args, logger)

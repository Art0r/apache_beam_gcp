"""
Dataflow Job/Pipeline to retrieve data from Posgresql and transform then upload to BigQuery
"""
from curso_apache_beam_gcp.pipeline_laucher import run_pipeline
from dotenv import load_dotenv
import logging
import sys


logger = logging.getLogger()

if __name__ == "__main__":
    load_dotenv()
    pipeline_args = sys.argv[1:]
    run_pipeline(pipeline_args, logger)

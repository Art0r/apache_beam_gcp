"""
Dataflow Job/Pipeline to retrieve data from Posgresql and transform then upload to BigQuery
"""
from curso_apache_beam_gcp.pipeline_laucher import run_pipeline
from curso_apache_beam_gcp.env_vars import init_env_vars
import logging
import os

logger = logging.getLogger()

if __name__ == "__main__":
    import sys
    pipeline_args = sys.argv[1:]
    run_pipeline(pipeline_args, logger)

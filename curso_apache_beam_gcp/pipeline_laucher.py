import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from curso_apache_beam_gcp.env_vars import init_env_vars
from curso_apache_beam_gcp.pipelines import mongo_to_bq, postgres_to_bq


def run_pipeline(pipeline_args: list[str], logger: logging.Logger) -> None:

    init_env_vars()
    logger.info("Getting environment variables")

    is_from_direct_runner = '--runner=DirectRunner' in pipeline_args

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(
        options=pipeline_options
    ) as p:

        mongo_to_bq.run_pipeline(p, is_from_direct_runner, logger)

        postgres_to_bq.run_pipeline(p, is_from_direct_runner, logger)

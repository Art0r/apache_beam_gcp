"""
Dataflow Job/Pipeline to retrieve data from Posgresql and transform then upload to BigQuery
"""
from curso_apache_beam_gcp_ext_lib.db import connection
from sqlalchemy import text

with connection.connect_with_connector().connect() as conn:
    result = conn.execute(text("SELECT * FROM users"))
    print(result.fetchall())

# from curso_apache_beam_gcp_ext_lib.pipeline_laucher import run_pipeline

# if __name__ == "__main__":
#     import sys
#     pipeline_args = sys.argv[1:]
#     run_pipeline(pipeline_args)

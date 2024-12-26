import os

from google.cloud.sql.connector import Connector, IPTypes
import pg8000

import sqlalchemy
from curso_apache_beam_gcp.env_vars import find_value_for_keys


def connect_with_postgres_database() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of Postgres.
    Uses the Cloud SQL Python Connector package.
    """

    ip_type = IPTypes.PRIVATE if os.environ.get(
        "PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    secrets = find_value_for_keys(
        "PSQL_INSTANCE_NAME", "PSQL_USER", "PSQL_PASSWORD", "PSQL_NAME")

    instance_connection_string = secrets.get('PSQL_INSTANCE_NAME', '')
    user = secrets.get('PSQL_USER')
    password = secrets.get('PSQL_PASSWORD')
    db = secrets.get('PSQL_NAME')

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_string,
            "pg8000",
            user=user,
            password=password,
            db=db,
            ip_type=ip_type,
        )
        return conn

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=5,  # Adjust pool size as needed
        max_overflow=2,
    )

    return pool

import os

from google.cloud.sql.connector import Connector, IPTypes
import pg8000

import sqlalchemy


def connect_with_postgres_database() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of Postgres.
    Uses the Cloud SQL Python Connector package.
    """

    ip_type = IPTypes.PRIVATE if os.environ.get(
        "PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_string=os.environ.get("PSQL_INSTANCE_NAME"),
            driver="pg8000",
            user=os.environ.get('PSQL_USER'),
            password=os.environ.get('PSQL_PASSWORD'),
            db=os.environ.get('PSQL_NAME'),
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

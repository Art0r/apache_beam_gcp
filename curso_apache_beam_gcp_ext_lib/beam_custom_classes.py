"""
DoFn classes
"""
import apache_beam as beam
from curso_apache_beam_gcp_ext_lib.db import connection
from sqlalchemy import text

class ReadUsersFromPostgres(beam.DoFn):
    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)

    def setup(self):
        pass

    def process(self, element):
        with connection.connect_with_database().connect() as conn:
            result = conn.execute(text("SELECT * FROM users"))
            yield from result.fetchall()
            conn.close()

    def teardown(self):
        pass

class FormatUsersToBq(beam.DoFn):
    def process(self, element):
        name, email, age = element

        if not isinstance(name, str) or not name.strip():
            name = "Unknown"
        if not isinstance(email, str) or "@" not in email or "." not in email or "@." in email or ".@" in email:
            email = "invalid@example.com"
        if not isinstance(age, int) or age <= 0:
            age = None

        yield {"Name": name, "Email": email, "Age": age}

"""
DoFn classes
"""
import random
import apache_beam as beam
from curso_apache_beam_gcp_ext_lib.db import connection
from sqlalchemy import text

class ReadUsersFromPostgres(beam.DoFn):
    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)

    def setup(self):
        pass

    def process(self, element):
        batch_size = 1000
        
        base_query = text("SELECT * FROM users LIMIT :batch_size OFFSET :offset")
        
        with connection.connect_with_postgres_database().connect() as conn:
            offset = 0

            while True:

                result = conn.execute(base_query, { "batch_size": batch_size, "offset": offset })
                rows = result.fetchall()
                
                # if nothing is returned it means that we read the all the rows
                if not rows:
                    break
                
                yield from rows
                
                offset += batch_size
            
            conn.close()

    def teardown(self):
        pass

class FormatUsersFromMongoDbToBq(beam.DoFn):
    def process(self, element: dict):
        name = element.get('name') 
        email = element.get('email') 
        
        if not isinstance(name, str) or not name.strip():
            name = "Unknown"
        if not isinstance(email, str) or "@" not in email or "." not in email or "@." in email or ".@" in email:
            email = "invalid@example.com"

        # if email is @gameofthron.es then return must be none, so de elemente wont pass ahead in the pipeline
        if email.__contains__('@gameofthron.es'):
            return

        yield {"name": name, "email": email, "age": random.randint(0, 80)}


class FormatUsersFromPostgresToBq(beam.DoFn):
    def process(self, element):
        name, email, age = element

        if not isinstance(name, str) or not name.strip():
            name = "Unknown"
        if not isinstance(email, str) or "@" not in email or "." not in email or "@." in email or ".@" in email:
            email = "invalid@example.com"
        if not isinstance(age, int) or age <= 0:
            age = None

        yield {"name": name, "email": email, "age": age}

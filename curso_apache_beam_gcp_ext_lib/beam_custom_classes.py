"""
DoFn classes
"""
import psycopg2 as pg
import apache_beam as beam


class ReadUsersFromPostgres(beam.DoFn):
    def __init__(self, db_config: dict, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.db_config = db_config
        self._conn = None
        self._cursor = None

    def setup(self):
        self._conn = pg.connect(**self.db_config)
        self._cursor = self._conn.cursor()

    def process(self, element):
        self._cursor.execute("SELECT * FROM users")
        yield from self._cursor.fetchall()

    def teardown(self):
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_conn"] = None
        state["_cursor"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)


class FormatUsersToBq(beam.DoFn):
    def process(self, element):
        name, email, age = element

        if not isinstance(name, str) or not name.strip():
            name = "Unknown"
        if not isinstance(email, str) or "@" not in email or "." not in email:
            email = "invalid@example.com"
        if not isinstance(age, int) or age <= 0:
            age = None

        yield {"Name": name, "Email": email, "Age": age}

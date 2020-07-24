"""


Tables created:
    * raw_table - raw import of entire CSV file
    * donors - all distinct donors based on name and address
    * recipients - all distinct campaign contribution recipients
    * contributions - contribution amounts tied to donor and recipients tables
"""

from os import getenv
import psycopg2
import psycopg2.extras
from psycopg2 import DatabaseError, OperationalError, Error
import logging


DATABASE_NAME = getenv("POSTGRES_DB")
DATABASE_USER = getenv("POSTGRES_USER")
DATABASE_PASSWORD = getenv("PGPASSFILE")
DATABASE_HOST = getenv("POSTGRES_HOST")
DATABASE_PORT = getenv("POSTGRES_PORT")


log = logging.getLogger(__name__)


class Postgres:
    """PostgreSQL database class"""

    def __init__(self):
        self.dbname = DATABASE_NAME
        self.username = DATABASE_USER
        self.password = DATABASE_PASSWORD
        self.host = DATABASE_HOST
        self.port = DATABASE_PORT
        self.con = None


    def connect(self):
        """Connect to a PostgreSQL database. """
        if self.con is None:
            try:
                self.con = psycopg2.connect(db=self.dbname,
                                            user=self.username,
                                            password=self.password,
                                            host=self.host,
                                            port=self.port)
                self.con.autocommit = True
            except DatabaseError as e:
                print(f"The error {e} occured")
                log.error(e)
                raise e
            finally:
                log.info('Connection opened successfully.')


    def create_database(self, query):
        """Create a database to store the Illinois_campaign_contributions dataset.

        :param query:
        """
        self.connect()
        with self.con.cursor() as cur:
            try:
                cur.execute(query)
                self.con.commit()
                print("Database created sucessfully")
            except OperationalError as e:
                print(f"The error {e} occured")


    def execute_query(self, query):
        self.connect()
        with self.con.cursor() as cur:
            try:
                cur.execute(query)
                self.con.commit()
                print("Query executed sucessfully")
            except Error as e:
                print(f"The error {e} occured")


    def copy_csv_query(self, query, csv_file):
        self.connect()
        with self.con.cursor() as cur:
            # cur.execute(query)
            cur.copy_expert(query, csv_file)
            self.con.commit()
            return f"{cur.rowcount} rows affected"


    def fetchall_query(self, query):
        self.connect()
        with self.con.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()
            return records


    def update_rows(self, query):
        self.connect()
        with self.con.cursor() as cur:
            cur.execute(query)
            self.con.commit()
            return f"{cur.rowcount} rows affected"

db_obj = Postgres()
print(db_obj.username)
print(db_obj.dbname)
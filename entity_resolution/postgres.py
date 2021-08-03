#!/usr/bin/env python3
"""
Creates a Postgresql database class.

Tables created:
    * raw_table - raw import of entire CSV file
    * donors - all distinct donors based on name and address
    * recipients - all distinct campaign contribution recipients
    * contributions - contribution amounts tied to donor and recipients tables

"""
from os import getenv
from os import path
import psycopg2
import psycopg2.extras
from psycopg2.errors import DatabaseError, OperationalError, SyntaxError
from logging import exception
import pandas as pd
pd.set_option('display.max_columns', None)

from string_grouper import match_strings, group_similar_strings, StringGrouper

DATABASE_NAME = getenv("POSTGRES_DB")
DATABASE_USER = getenv("POSTGRES_USER")
DATABASE_PASSWORD = getenv("PGPASSFILE")
DATABASE_HOST = getenv("POSTGRES_HOST")
DATABASE_PORT = getenv("POSTGRES_PORT")
DATABASE_SCHEMA_NAME = 'il_campaign_contribution'

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'
CONTRIBUTIONS_PATH = ABS_PATH + EXTERNAL_DATA_DIR

_file = 'Illinois-campaign-contributions'
contributions_csv_file = CONTRIBUTIONS_PATH + _file + '.csv'


# TODO: Complete PostgresConnection
# class PostgresConnection:
#
#     def __init__(self):
#         self.dbname = DATABASE_NAME
#         self.username = DATABASE_USER
#         self.password = DATABASE_PASSWORD
#         self.host = DATABASE_HOST
#         self.port = DATABASE_PORT
#         self.con = None
#         self.schema = DATABASE_SCHEMA_NAME
#         self.csv_file = None
#
#     def cursur(self):
#         """Returns a cursor object and starts a new transaction"""
#         pass
#     def commit(self):
#         """Commits current transaction"""
#         pass
#     def rollback(self):
#         """Rolls back current transaction"""
#         pass
#
#     def __enter__(self):
#         cursor = self.cursor()
#         return cursor
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if exc_tb is None:
#             #No exception, so commit
#             self.commit()
#         else:
#             #Exception occured, so rollback
#             self.rollback()



class Postgres:
    """PostgreSQL database class."""

    def __init__(self):
        self.dbname = DATABASE_NAME
        self.username = DATABASE_USER
        self.password = DATABASE_PASSWORD
        self.host = DATABASE_HOST
        self.port = DATABASE_PORT
        self.con = None
        self.schema = DATABASE_SCHEMA_NAME
        self.csv_file = None


    def connect(self):
        """Connect to a PostgreSQL database."""

        if self.con is None:

            try:
                self.con = psycopg2.connect(db=self.dbname,
                                            user=self.username,
                                            password=self.password,
                                            host=self.host,
                                            port=self.port,
                                            options=f'-c search_path={self.schema}')
                self.con.autocommit = True

            except (DatabaseError, AttributeError, Exception) as e:
                # print(f'Error: {e}')
                exception(f'Connection request failed: {e}')



    def execute_query(self, query: str):
        """Executes a query."""

        # self.connect()
        self.connect()
        with self.con.cursor() as cur:
            try:
                cur.execute(query)
                self.con.commit()
                # print("Query executed sucessfully")
            # except SyntaxError:
                # self.logs.error("execute_query failed: %s" % query)
            except DatabaseError as e:
                print(f'Error: {e}')
                exception(f'Database Error: {e}')

            else:
                print('Query executed')


    def copy_csv_query(self):
        """Copies data from a csv to a table."""
        copy_raw_table = ('''
                COPY raw_table 
                    (reciept_id, last_name, first_name, 
                    address_1, address_2, city, state, 
                    zip, report_type, date_recieved, 
                    loan_amount, amount, receipt_type, 
                    employer, occupation, vendor_last_name, 
                    vendor_first_name, vendor_address_1, 
                    vendor_address_2, vendor_city, vendor_state, 
                    vendor_zip, description, election_type, 
                    election_year, 
                    report_period_begin, report_period_end, 
                    committee_name, committee_id) 
                FROM STDIN CSV HEADER;
        ''')
        try:
            csv_file = open(contributions_csv_file, 'r')
        except (EOFError, IOError, FileNotFoundError) as error:
            print('Error: {}'.format(error))
            raise error


        self.connect()

        with self.con.cursor() as cur:
            cur.copy_expert(copy_raw_table, csv_file)
            csv_file.close()
            self.con.commit()
            cur.close()
            rowcount = cur.rowcount
            return "{} rows affected".format(rowcount)


    def fetchall_query(self, query: str):
        """Returns all records."""

        self.connect()
        with self.con.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()
            cur.close()
            return records


    def drop_table(self):
        """Drops tables"""
        drop_raw_table = ('''
                DROP TABLE IF EXISTS raw_table;
            ''')
        drop_donors = ('''
                DROP TABLE IF EXISTS donors;
            ''')
        drop_recipients = ('''
                DROP TABLE IF EXISTS recipients;
            ''')
        drop_contributions = ('''
                DROP TABLE IF EXISTS contributions;
            ''')
        drop_processed_donors = ('''
                DROP TABLE IF EXISTS processed_donors;
            ''')

        drop_queries = [drop_raw_table, drop_donors, drop_recipients, drop_contributions, drop_processed_donors]

        self.connect()
        with self.con.cursor() as cur:
            try:
                for drop_query in drop_queries:
                    cur.execute(drop_query)

                self.con.commit()

            except DatabaseError as e:
                print(f"Error: {e}")
            else:
                print("Tables Dropped")



    def create_table(self):
        """

        :return:
        """
        create_raw_table = ('''
                CREATE TABLE IF NOT EXISTS raw_table
                    (reciept_id INT, last_name VARCHAR(70), first_name VARCHAR(35),
                    address_1 VARCHAR(35), address_2 VARCHAR(36), city VARCHAR(20),
                    state VARCHAR(15), zip VARCHAR(11), report_type VARCHAR(24),
                    date_recieved VARCHAR(10), loan_amount VARCHAR(12),
                    amount VARCHAR(23), receipt_type VARCHAR(23),
                    employer VARCHAR(70), occupation VARCHAR(40),
                    vendor_last_name VARCHAR(70), vendor_first_name VARCHAR(20),
                    vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31),
                    vendor_city VARCHAR(20), vendor_state VARCHAR(10),
                    vendor_zip VARCHAR(10), description VARCHAR(90),
                    election_type VARCHAR(10), election_year VARCHAR(10),
                    report_period_begin VARCHAR(10), report_period_end VARCHAR(33),
                    committee_name VARCHAR(70), committee_id VARCHAR(37));
        ''')

        create_donors = ('''
                CREATE TABLE IF NOT EXISTS donors 
                    (donor_id SERIAL PRIMARY KEY, 
                    last_name VARCHAR(70), first_name VARCHAR(35), 
                    address_1 VARCHAR(35), address_2 VARCHAR(36), 
                    city VARCHAR(20), state VARCHAR(15), 
                    zip VARCHAR(11), employer VARCHAR(70), 
                    occupation VARCHAR(40));
        ''')

        create_recipients = ('''
                CREATE TABLE IF NOT EXISTS recipients 
                    (recipient_id SERIAL PRIMARY KEY, name VARCHAR(70));
        ''')

        create_table_contributions = ('''
                CREATE TABLE IF NOT EXISTS contributions 
                    (contribution_id INT, donor_id INT, recipient_id INT, 
                    report_type VARCHAR(24), date_recieved DATE, 
                    loan_amount VARCHAR(12), amount VARCHAR(23), 
                    receipt_type VARCHAR(23), 
                    vendor_last_name VARCHAR(70), 
                    vendor_first_name VARCHAR(20), 
                    vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31), 
                    vendor_city VARCHAR(20), vendor_state VARCHAR(10), 
                    vendor_zip VARCHAR(10), description VARCHAR(90), 
                    election_type VARCHAR(10), election_year VARCHAR(10), 
                    report_period_begin DATE, report_period_end DATE);
        ''')

        # create_table_queries = [create_raw_table, create_donors, create_recipients, create_table_contributions ]
        self.connect()
        with self.con.cursor() as cur:
            try:
                cur.execute(create_raw_table)
                cur.execute(create_donors)
                cur.execute(create_recipients)
                cur.execute(create_table_contributions)

                self.con.commit()

            except DatabaseError as e:
                print(f"Error: {e}")
            else:
                print("Tables Created")



    def insert(self):
        """

        :return:
        """
        insert_into_donors = ('''
                INSERT INTO donors 
                    (first_name, last_name, address_1, 
                    address_2, city, state, zip, employer, occupation) 
                    SELECT 
                        DISTINCT 
                        LOWER(TRIM(first_name)), LOWER(TRIM(last_name)), 
                        LOWER(TRIM(address_1)), LOWER(TRIM(address_2)), 
                        LOWER(TRIM(city)), LOWER(TRIM(state)), LOWER(TRIM(zip)), 
                        LOWER(TRIM(employer)), LOWER(TRIM(occupation)) 
                FROM raw_table;
        ''')

        insert_into_recipients = ('''
                INSERT INTO recipients 
                    SELECT DISTINCT CAST(committee_id AS INTEGER), 
                    committee_name 
                FROM raw_table;''')

        insert_into_contributions = ('''
                INSERT INTO contributions 
                SELECT 
                    reciept_id, donors.donor_id, CAST(committee_id AS INTEGER), 
                    report_type, TO_DATE(TRIM(date_recieved), 'MM/DD/YYYY'), 
                    loan_amount, amount, 
                    receipt_type, vendor_last_name , 
                    vendor_first_name, vendor_address_1,
                    vendor_address_2, 
                    vendor_city, vendor_state, vendor_zip,
                    description, 
                    election_type, election_year, 
                    TO_DATE(TRIM(report_period_begin), 'MM/DD/YYYY'), 
                    TO_DATE(TRIM(report_period_end), 'MM/DD/YYYY') 
                FROM raw_table JOIN donors ON 
                    donors.first_name = LOWER(TRIM(raw_table.first_name)) AND 
                    donors.last_name = LOWER(TRIM(raw_table.last_name)) AND 
                    donors.address_1 = LOWER(TRIM(raw_table.address_1)) AND 
                    donors.address_2 = LOWER(TRIM(raw_table.address_2)) AND 
                    donors.city = LOWER(TRIM(raw_table.city)) AND 
                    donors.state = LOWER(TRIM(raw_table.state)) AND 
                    donors.employer = LOWER(TRIM(raw_table.employer)) AND 
                    donors.occupation = LOWER(TRIM(raw_table.occupation)) AND 
                    donors.zip = LOWER(TRIM(raw_table.zip));
        ''')

        self.connect()
        with self.con.cursor() as cur:
            cur.execute(insert_into_donors)
            cur.execute(insert_into_recipients)
            cur.execute(insert_into_contributions)

            self.con.commit()
            cur.close()
            rowcount = cur.rowcount
            return "{} rows affected".format(rowcount)


    def create_index(self):
        """

        :return:
        """
        create_index_donors = ('''
                CREATE INDEX donors_donor_info 
                    ON donors 
                (last_name, first_name, address_1, address_2, city, 
                state, zip);
        ''')
        create_index_contributions_prkey = ('''
                ALTER TABLE contributions ADD PRIMARY KEY(contribution_id);
        ''')
        create_index_contributions_1 = ('''
                CREATE INDEX donor_idx ON contributions (donor_id);
        ''')
        create_index_contributions_2 = ('''
                CREATE INDEX recipient_idx ON contributions (recipient_id);
        ''')
        self.execute_query(create_index_donors)
        self.execute_query(create_index_contributions_prkey)
        self.execute_query(create_index_contributions_1)
        self.execute_query(create_index_contributions_2)



    def update_rows(self):
        """Updates Postgresql records."""

        nullify_in_donors = ('''
                UPDATE donors 
                    SET 
                    first_name = CASE first_name WHEN '' THEN NULL ELSE first_name END, 
                    last_name = CASE last_name WHEN '' THEN NULL ELSE last_name END, 
                    address_1 = CASE address_1 WHEN '' THEN NULL ELSE address_1 END, 
                    address_2 = CASE address_2 WHEN '' THEN NULL ELSE address_2 END, 
                    city = CASE city WHEN '' THEN NULL ELSE city END, 
                    state = CASE state WHEN '' THEN NULL ELSE state END, 
                    employer = CASE employer WHEN '' THEN NULL ELSE employer END, 
                    occupation = CASE occupation WHEN '' THEN NULL ELSE occupation END, 
                    zip = CASE zip WHEN '' THEN NULL ELSE zip END;
        ''')

        self.connect()
        with self.con.cursor() as cur:
            cur.execute(nullify_in_donors)
            self.con.commit()
            cur.close()
            row_count = cur.rowcount
            return "{} rows affected".format(row_count)


    def process_donors_table(self):
        """

        :return:
        """
        create_processed_donors = ('''
                CREATE TABLE processed_donors AS 
                (SELECT donor_id, 
                LOWER(city) AS city, 
                CASE WHEN (first_name IS NULL AND last_name IS NULL) 
                      THEN NULL 
                    ELSE LOWER(CONCAT_WS(' ', first_name, last_name)) 
                END AS name, 
                LOWER(zip) AS zip, 
                LOWER(state) AS state, 
                CASE WHEN (address_1 IS NULL AND address_2 IS NULL) 
                      THEN NULL 
                     ELSE LOWER(CONCAT_WS(' ', address_1, address_2)) 
                END AS address, 
                LOWER(occupation) AS occupation, 
                LOWER(employer) AS employer, 
                CAST((first_name IS NULL) AS INTEGER) AS person 
                FROM donors);
        ''')
        create_pd_index = ('''
                CREATE INDEX processed_donor_idx ON processed_donors (donor_id);
        ''')

        self.execute_query(create_processed_donors)
        self.execute_query(create_pd_index)


    def get_query_statistics(self):
        """
        source: https://www.postgresql.eu/events/nordicpgday2018/sessions/session/1858/slides/68/query-optimization-techniques_talk.pdf
        :return:
        """
        query_statistics = ('''
           WITH ttl AS (
                SELECT 
                    sum(total_time) AS total_time,
                    sum(blk_read_time + blk_write_time) AS io_time,
                    sum(total_time - blk_read_time - blk_write_time) AS cpu_time,
                    sum(calls) AS ncalls,
                    sum(rows) AS total_rows
                FROM 
                    pg_stat_statements
                WHERE 
                    dbid IN (SELECT oid FROM pg_database WHERE datname=current_database())
                )
                SELECT *,(pss.total_time - pss.blk_read_time - pss.blk_write_time)/ttl.cpu_time >= 0.05
                ORDER BY pss.total_time - pss.blk_read_time - pss.blk_write_time DESC LIMIT 1;
        ''')
        self.execute_query(query_statistics)


    def get_partial_duplicates(self):
        """Uses string_grouper to groups similar strings and return records in a DataFrame.

        :return: df
        """
        self.connect()
        # df = pd.read_sql_query('SELECT donor_id, name '
        #                        'FROM processed_donors '
        #                        'LIMIT 1000; ', con=self.con)

        df = pd.read_sql_query('SELECT donor_id, name '
                               'FROM processed_donors; ', con=self.con)

        df['deduplicated_names'] = group_similar_strings(df['name'])
        # print(df.groupby('deduplicated_names').count().sort_values('donor_id', ascending=False).head(20)['donor_id'])

        # df = df.groupby('deduplicated_names').count().sort_values('donor_id', ascending=False).head(20)['donor_id']
        grouped_df = df.groupby('deduplicated_names').count().sort_values('donor_id', ascending=False)['donor_id']

        grouped_nan_count = df.deduplicated_names.isna().sum()

        grouped_unique_count = df.deduplicated_names.nunique()
        grouped_total_count = len(df)
        grouped_duplicate_count = grouped_total_count - grouped_unique_count
        return grouped_unique_count, grouped_duplicate_count, grouped_total_count, grouped_nan_count


    def get_exact_duplicates(self):
        """Uses GROUP BY to identify duplicate records and return the output in a DataFrame.

        """
        self.connect()

        # df = pd.read_sql_query('SELECT name '
        #                        'FROM processed_donors '
        #                        'GROUP BY name; ', con=self.con)


        df = pd.read_sql_query('''
                            SELECT name, 
                            count(*) 
                            FROM processed_donors 
                            GROUP BY name
                            ORDER BY count(*) DESC;''', con=self.con)

        df = df.head(20)
        return df

    def get_cmdline_query(self, cmdline_query:str):
        """

        :param query:
        :return:
        """
        self.connect()
        # tested queries:
        # python main.py -q "SELECT count(distinct name) FROM processed_donors;"
        # python main.py -q "SELECT (count(name) - count(distinct name)) as duplicate_count FROM processed_donors;"
        # python main.py -q "SELECT count(*) FROM processed_donors WHERE name IS NULL;"

        df = pd.read_sql_query('''%s''' % cmdline_query, con=self.con)

        df = df.head(50)
        return df
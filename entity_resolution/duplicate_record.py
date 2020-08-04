#!/usr/bin/env python3
"""

Reference library: https://github.com/Bergvca/string_grouper

"""
import pandas as pd
from string_grouper import match_strings, group_similar_strings, StringGrouper
import psycopg2
from psycopg2.errors import DatabaseError, OperationalError, SyntaxError
pd.set_option('display.max_columns', None)


from postgres import DATABASE_NAME
from postgres import DATABASE_USER
from postgres import DATABASE_PASSWORD
from postgres import DATABASE_HOST
from postgres import DATABASE_PORT
from postgres import DATABASE_SCHEMA_NAME


class DuplicateRecord:
    """Duplicate records to compare string_grouper vs SQL GROUP BY """

    def __init__(self):
        self.dbname = DATABASE_NAME
        self.username = DATABASE_USER
        self.password = DATABASE_PASSWORD
        self.host = DATABASE_HOST
        self.port = DATABASE_PORT
        self.con = None
        self.schema = DATABASE_SCHEMA_NAME

    def connect(self):
        """Connect to a PostgreSQL database."""

        if self.con is None:
            try:
                self.con = psycopg2.connect(db=self.dbname,
                                            user=self.username,
                                            password=self.password,
                                            host=self.host,
                                            port=self.port,
                                            options=f"-c search_path={self.schema}")
            except DatabaseError as e:
                print(f"The error {e} occured")
                # self.logs.error("Connection request failed: %s" % e)
            except AttributeError as e:
                print(f"The error {e} occured")
                # self.logs.error("Connection request failed: %s" % e)
            finally:
                print(f"Connection opened successfully.")



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
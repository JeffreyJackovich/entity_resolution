import pandas as pd
from string_grouper import match_strings, group_similar_strings, StringGrouper
from time import time
import psycopg2
pd.set_option('display.max_columns', None)
from os import getenv


DATABASE_NAME = getenv("POSTGRES_DB")
DATABASE_USER = getenv("POSTGRES_USER")
DATABASE_PASSWORD = getenv("PGPASSFILE")
DATABASE_HOST = getenv("POSTGRES_HOST")
DATABASE_PORT = getenv("POSTGRES_PORT")


con_obj = psycopg2.connect(db=DATABASE_NAME,
                            user=DATABASE_USER,
                            password=DATABASE_PASSWORD,
                            host=DATABASE_HOST,
                            port=DATABASE_PORT)


def get_duplicates_sql():
    df = pd.read_sql_query('SELECT * '
                           'FROM processed_donors ', con=con_obj)



start_time = time()
df['deduplicated_names'] = group_similar_strings(df['name'])
print(df.groupby('deduplicated_names').count().sort_values('donor_id', ascending=False).head(20)['donor_id'])
print("--- %s seconds ---" % (time() - start_time))

if __name__ == '__main__':
    start_time = time()
    get_duplicates_sql()
    print("--- %s seconds ---" % (time() - start_time))
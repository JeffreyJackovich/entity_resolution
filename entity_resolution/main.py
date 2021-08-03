#!/usr/bin/env python3
"""


"""
__author__ = "Jeffrey Jackovich"
__version__ = "0.1.0"
__license__ = "MIT"

from dataset import Dataset
from dataset import DatasetReader
from os import path
from postgres import Postgres
from time import time
from argparse import ArgumentParser
from logging import basicConfig
from logging import DEBUG
from logging import info

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'

def main(args):
    basicConfig(filename=ABS_PATH + '/entity_resolution/log_file.log',
                format='%(asctime)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s',
                filemode='a', level=DEBUG)  # level=ERROR

    if args.get_dataset:
        #################################################################################
        # '-gd'
        #################################################################################
        info('Downloading the dataset..\n')
        source_dataset = Dataset(ABS_PATH + EXTERNAL_DATA_DIR)
        source_dataset.download_zipfile()
        source_dataset.unzip_zipfile()
        source_dataset.transform_txt_to_csv()

        info('Getting the dataset row count..')
        ds_reader = DatasetReader(ABS_PATH + EXTERNAL_DATA_DIR)
        info(f'\tDataset row count is: {len(ds_reader)}')


    elif args.setup_database:
        #################################################################################
        # '-sd'
        #################################################################################
        db = Postgres()
        info('Dropping tables if exist..\n')
        info(db.drop_table())

        info('Creating the tables..')
        info(db.create_table())

        info('Importing raw data from csv..')
        info(db.copy_csv_query())

        info('Inserting data into tables..')
        info(db.insert())

        info('Creating indexes on tables..')
        info(db.create_index())

        info('Nullifying empty strings in donors..')
        info(db.update_rows())

        db.process_donors_table()
        info('Done.')


    elif args.get_partial_duplicate:
        #################################################################################
        # '-gpd'
        #################################################################################
        info('Starting string_grouper...\n')
        dup_record = Postgres()

        unique, duplicate, total, nans = dup_record.get_partial_duplicates()
        info(f'Unique: {unique}, Duplicate: {duplicate}, Total: {total}, Nan: {nans}')


    elif args.get_exact_duplicate:
        #################################################################################
        # '-ged'
        #################################################################################
        info('Starting get_exact_duplicates...\n')
        dup_record = Postgres()

        info(dup_record.get_exact_duplicates())



    sql_query = args.sql_query
    #################################################################################
    # '-q'
    #################################################################################
    if sql_query:
        dr = Postgres()
        print(dr.get_cmdline_query(sql_query))




if __name__ == "__main__":
    parser = ArgumentParser(prog='entity_resolution',
                            description='Identify partial and exact duplicate records')

    parser.add_argument('-gd',
                        '--get_dataset',
                        action='store_true',
                        default=False,
                        help='Downloads and processes dataset: Illinois-campaign-contributions.txt.zip')

    parser.add_argument('-sd',
                        '--setup_database',
                        action='store_true',
                        default=False)

    parser.add_argument('-gpd',
                        '--get_partial_duplicate',
                        action='store_true',
                        default=False)

    parser.add_argument('-ged',
                        '--get_exact_duplicate',
                        action='store_true',
                        default=False)

    parser.add_argument('-q',
                        '--get_cmdline_query',
                        action='store',
                        dest='sql_query',
                        default=False,
                        type=str,
                        help='SQL query to execute.')



    start_time = time()
    try:
        args = parser.parse_args()
        main(args)
    except Exception as e:
        print(f'Error: {e}')
    finally:
        print('Done')
        print('--- %s seconds ---' % round((time() - start_time),2))
        print('--- %s minutes ---' % round((time() - start_time)/ 60,2))


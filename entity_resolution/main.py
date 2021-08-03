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
from logging import ERROR
from logging import info
from logging import exception

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'
CONTRIBUTIONS_PATH = ABS_PATH + EXTERNAL_DATA_DIR

_file = 'Illinois-campaign-contributions'
contributions_zip_file = CONTRIBUTIONS_PATH + _file + '.txt.zip'
contributions_txt_file = CONTRIBUTIONS_PATH + _file + '.txt'
contributions_csv_file = CONTRIBUTIONS_PATH + _file + '.csv'



# def get_dataset():
#     """
#
#     :return:
#     """
#     print('Downloading the dataset..\n')
#     source_dataset = Dataset(CONTRIBUTIONS_PATH)
#     source_dataset.download_file()
#
#     print('Getting the dataset row count..')
#     ds_reader = DatasetReader(contributions_csv_file)
#     print('\tDataset row count is: {} \n'.format(len(ds_reader)))
#     return ds_reader


def main(args):
    # print(args)

    basicConfig(filename=ABS_PATH + '/entity_resolution/entity_resolution/log/log_file.log',
                format='%(asctime)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s',
                filemode='a', level=DEBUG)  # level=ERROR

    if args.get_dataset:
        info('Downloading the dataset..\n')
        # print('Downloading the dataset..\n')
        source_dataset = Dataset(CONTRIBUTIONS_PATH)
        source_dataset.download_file()

        info('Getting the dataset row count..')
        # print('Getting the dataset row count..')
        ds_reader = DatasetReader(contributions_csv_file)
        info(f'\tDataset row count is: {len(ds_reader)}')
        # print('\tDataset row count is: {} \n'.format(len(ds_reader)))


    # start_time = time()
    # print('Starting (TO NOTE: Part_1 takes ~10.5 minutes to complete.)...\n')


    elif args.setup_database:
        db = Postgres()
        info('Dropping tables if exist..\n')
        # print('Dropping tables if exist..\n')
        info(db.drop_table())
        # print(db.drop_table())

        info('Creating the tables..\n')
        # print('Creating the tables..\n')
        # print('creating recipients table...')
        # print('creating contributions table...')
        info(db.create_table())
        # print(db.create_table())

        info('Importing raw data from csv...\n')
        info(db.copy_csv_query())
        # print('Importing raw data from csv...\n')
        # print(db.copy_csv_query())

        info('Inserting data into tables...\n')
        info(db.insert())
        # print('Inserting data into tables...\n')
        # print(db.insert())

        info('Creating indexes on tables...\n')
        # print('Creating indexes on tables...\n')
        # print('creating indexes on donors table...')
        # print('creating indexes on contributions...')
        info(db.create_index())

        info('Nullifying empty strings in donors...\n')
        info(db.update_rows())

        # print('creating processed_donors...')
        db.process_donors_table()
        # print("Database Init Duration: --- %s seconds ---" % (time() - start_time))


    elif args.get_partial_duplicate:
        info('Starting string_grouper...\n')
        dup_record = Postgres()
        # dup_record = DuplicateRecord()

        sg_start_time = time()
        unique, duplicate, total, nans = dup_record.get_partial_duplicates()
        info(f'Unique: {unique}, Duplicate: {duplicate}, Total: {total}, Nan: {nans}')
        print(f'string_grouper duration:--- {((time() - sg_start_time) / 60.0)} minutes ---')
        # print("Unique: {}, Duplicate: {}, Total: {}, Nan: {}".format(unique, duplicate, total, nans))
        # print("string_grouper duration:--- %s minutes ---" % ((time() - sg_start_time)/60.0))

    elif args.get_exact_duplicate:
        info('Starting get_exact_duplicates...\n')
        sql_start_time = time()
        dup_record = Postgres()
        # dup_record = DuplicateRecord()
        info(dup_record.get_exact_duplicates())
        info(f'sql Duration:--- %s{((time() - sql_start_time))} seconds ---')
        # print("sql Duration:--- %s seconds ---".format((time() - sql_start_time)))


    sql_query = args.sql_query
    if sql_query:
        dr = DuplicateRecord()
        print(dr.get_cmdline_query(sql_query))


    elif args.get_query_statistics:
        db = Postgres()
        db.get_query_statistics()


if __name__ == "__main__":
    parser = ArgumentParser(prog='entity_resolution',
                            description='Identify partial and exact duplicate records')
    # optional argument to get contributions dataset
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

    parser.add_argument('-gqs',
                        '--get_query_statistics',
                        action='store_true',
                        default=False)

    try:
        args = parser.parse_args()
        main(args)
    except Exception as e:
        print('Error: {}'.format(e))
    finally:
        print('Done')

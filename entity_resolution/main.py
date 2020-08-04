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
from duplicate_record import DuplicateRecord
from argparse import ArgumentParser

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'
CONTRIBUTIONS_PATH = ABS_PATH + EXTERNAL_DATA_DIR


# TODO LOGS_FORMAT = '%(asctime)s ; %(name)s ; %(process)d ; %(thread)d ; %(levelname)s ; %(message)s'


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


    if args.get_dataset:
        print('Downloading the dataset..\n')
        source_dataset = Dataset(CONTRIBUTIONS_PATH)
        source_dataset.download_file()

        print('Getting the dataset row count..')
        ds_reader = DatasetReader(contributions_csv_file)
        print('\tDataset row count is: {} \n'.format(len(ds_reader)))


    # start_time = time()
    # print('Starting (TO NOTE: Part_1 takes ~10.5 minutes to complete.)...\n')
    ###########################################################################
    # TODO logging.basicConfig(format=LOGS_FORMAT,
    #                     datefmt='%d-%m-%Y %H:%M:%S',
    #                     filename="tmp/entity_resolution.log",
    #                     filemode='w',
    #                     level=logging.DEBUG)
    #
    # logging.info('INFO test log message')
    # logging.debug('debug test log message')
    ###########################################################################


    elif args.setup_database:
        db = Postgres()
        print('Dropping tables if exist..\n')
        print(db.drop_table())

        print('Creating the tables..\n')
        # print('creating recipients table...')
        # print('creating contributions table...')
        print(db.create_table())

        print('Importing raw data from csv...\n')
        print(db.copy_csv_query())

        print('Inserting data into tables...\n')
        print(db.insert())

        print('Creating indexes on tables...\n')
        # print('creating indexes on donors table...')
        # print('creating indexes on contributions...')
        print(db.create_index())

        print('Nullifying empty strings in donors...\n')
        print(db.update_rows())

        # print('creating processed_donors...')
        db.process_donors_table()
        # print("Database Init Duration: --- %s seconds ---" % (time() - start_time))


    elif args.get_partial_duplicate:
        print('Starting string_grouper...\n')
        dup_record = DuplicateRecord()
        sg_start_time = time()
        unique, duplicate, total, nans = dup_record.get_partial_duplicates()
        print("Unique: {}, Duplicate: {}, Total: {}, Nan: {}".format(unique, duplicate, total, nans))
        print("string_grouper duration:--- %s minutes ---" % ((time() - sg_start_time)/60.0))

    elif args.get_exact_duplicate:
        print('Starting get_exact_duplicates...\n')
        sql_start_time = time()
        dup_record = DuplicateRecord()
        print(dup_record.get_exact_duplicates())
        print("sql Duration:--- %s seconds ---".format((time() - sql_start_time)))


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

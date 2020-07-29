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
import argparse

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'
CONTRIBUTIONS_PATH = ABS_PATH + EXTERNAL_DATA_DIR


# TODO LOGS_FORMAT = '%(asctime)s ; %(name)s ; %(process)d ; %(thread)d ; %(levelname)s ; %(message)s'


_file = 'Illinois-campaign-contributions'
contributions_zip_file = CONTRIBUTIONS_PATH + _file + '.txt.zip'
contributions_txt_file = CONTRIBUTIONS_PATH + _file + '.txt'
contributions_csv_file = CONTRIBUTIONS_PATH + _file + '.csv'

# def get_parser():
#     """
#
#     :return:
#     """
#     parser = argparse.ArgumentParser()
#     author = __author__
#     parser.add_argument('--get_dataset', type=get_dataset)
#     return parser


def get_dataset():
    """

    :return:
    """
    print('Downloading the dataset..\n')
    source_dataset = Dataset(CONTRIBUTIONS_PATH)
    source_dataset.download_file()

    print('Getting the dataset row count..')
    ds_reader = DatasetReader(contributions_csv_file)
    print('\tDataset row count is: {} \n'.format(len(ds_reader)))


def main(args):
    print("hello world")
    print(args)


    # start_time = time()
    # print('Starting (TO NOTE: Part_1 takes ~10.5 minutes to complete.)...\n')
    # TODO logging.basicConfig(format=LOGS_FORMAT,
    #                     datefmt='%d-%m-%Y %H:%M:%S',
    #                     filename="tmp/entity_resolution.log",
    #                     filemode='w',
    #                     level=logging.DEBUG)
    #
    # logging.info('INFO test log message')
    # logging.debug('debug test log message')

    # print('Downloading the dataset..\n')
    # source_dataset = Dataset(CONTRIBUTIONS_PATH)
    # source_dataset.download_file()
    #
    # print('Getting the dataset row count..')
    # ds_reader = DatasetReader(contributions_csv_file)
    # print('\tDataset row count is: {} \n'.format(len(ds_reader)))



    # db = Postgres()
    # print('Dropping tables if exist..')
    # db.drop_table()
    #
    # print('Creating the tables..')
    # # print('creating recipients table...')
    # # print('creating contributions table...')
    # db.create_table()
    #
    # print('Importing raw data from csv...\n')
    # db.copy_csv_query()
    #
    # print('Inserting data into tables...\n')
    # db.insert()
    #
    # print('Creating indexes on tables...\n')
    # # print('creating indexes on donors table...')
    # # print('creating indexes on contributions...')
    # db.create_index()
    #
    # print('Nullifying empty strings in donors...\n')
    # db.update_rows()
    #
    # # print('creating processed_donors...')
    # db.process_donors_table()
    #
    # # db.close()
    # print("Database Init Duration: --- %s seconds ---" % (time() - start_time))
    #
    #
    # print('Starting string_grouper...\n')
    # # get_partial_duplicates, get_exact_duplicates
    # dup_record = DuplicateRecord()
    # sg_start_time = time()
    # print(dup_record.get_partial_duplicates())
    # print("string_grouper Duration:--- %s seconds ---" % (time() - sg_start_time))
    #
    # sql_start_time = time()
    # print(dup_record.get_exact_duplicates())
    # print("sql Duration:--- %s seconds ---" % (time() - sql_start_time))
    #

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    
    author = __author__
    # parser.add_argument('--get_dataset', type=get_dataset)
    parser.add_argument()
    parser = get_parser()
    args = parser.parse_args(args)

    main(args)
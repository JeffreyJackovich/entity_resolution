"""
Setup script to download Illinois-campaign-contributions.txt.zip and convert to Illinois-campaign-contributions.txt


"""
from os import path
import logging

import csv
import zipfile
import requests
import unidecode


log = logging.getLogger(__name__)

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'
CONTRIBUTIONS_PATH = ABS_PATH + EXTERNAL_DATA_DIR


_file = 'Illinois-campaign-contributions'
contributions_zip_file = CONTRIBUTIONS_PATH + _file + '.txt.zip'
contributions_txt_file = CONTRIBUTIONS_PATH + _file + '.txt'
contributions_csv_file = CONTRIBUTIONS_PATH + _file + '.csv'


class Dataset:
    """A helper to process the Illinois-campaign-contributions dataset."""

    def __init__(self, CONTRIBUTIONS_PATH):
        self.CONTRIBUTIONS_PATH = CONTRIBUTIONS_PATH

    def download_file(self):
        """Downloads Illinois-campaign-contributions.txt.zip.

        :return:
        """
        if not path.exists(contributions_zip_file):
            print('downloading', contributions_zip_file, '(~60mb) ...')
            u = requests.get(
                'https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip')
            localFile = open(contributions_zip_file, 'wb')
            localFile.write(u.content)
            localFile.close()
        else:
            print(f"{contributions_zip_file[91:]} already downloaded")

    def extract_file(self):
        """

        :return:
        """
        if not path.exists(contributions_txt_file):
            zip_file = zipfile.ZipFile(contributions_zip_file, 'r')
            print('extracting %s' % contributions_zip_file)
            zip_file_contents = zip_file.namelist()
            for f in zip_file_contents:
                if ('.txt' in f):
                    zip_file.extract(f)
            zip_file.close()
        else:
            print(f"{contributions_txt_file[91:]} already extracted")


    def convert_file(self):
        """
        Converts the tab-delimited raw file to csv
        :return:
        """
        if not path.exists(contributions_csv_file):
            print('converting tab-delimited raw file to csv...')
            with open(contributions_txt_file, 'rU') as txt_file, \
                    open(contributions_csv_file, 'w') as csv_file:
                csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                for line in txt_file:
                    if not all(ord(c) < 128 for c in line):
                        line = unidecode.unidecode(line)
                    row = line.rstrip('\t\r\n').split('\t')
                    if len(row) != 29:
                        print('skipping bad row (length %s, expected 29):' % len(row))
                        print(row)
                        continue
                    csv_writer.writerow(row)
        else:
            print(f"{contributions_csv_file[91:]} already downloaded")

        return contributions_txt_file



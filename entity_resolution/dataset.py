#!/usr/bin/env python3
"""
Setup script to download Illinois-campaign-contributions.txt.zip
and convert it to Illinois-campaign-contributions.txt

"""
from os import path
import csv
import zipfile
import requests
import unidecode
from collections import Counter

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'
CONTRIBUTIONS_PATH = ABS_PATH + EXTERNAL_DATA_DIR


_file = 'Illinois-campaign-contributions'
contributions_zip_file = CONTRIBUTIONS_PATH + _file + '.txt.zip'
contributions_txt_file = CONTRIBUTIONS_PATH + _file + '.txt'
contributions_csv_file = CONTRIBUTIONS_PATH + _file + '.csv'


class Dataset:
    """A helper to process the Illinois-campaign-contributions dataset."""

    def __init__(self, CONTRIBUTIONS_PATH: str):
        self.CONTRIBUTIONS_PATH = CONTRIBUTIONS_PATH


    def download_file(self):
        """Downloads Illinois-campaign-contributions.txt.zip.
            Unzips a zipfile to txt format.
            Converts the tab-delimited raw file to csv.
        """

        if not path.exists(contributions_zip_file):
            print('downloading', contributions_zip_file, '(~60mb) ...')
            u = requests.get(
                'https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip')
            localFile = open(contributions_zip_file, 'wb')
            localFile.write(u.content)
            localFile.close()
        else:
            print(f"\tAlready downloaded file: {contributions_zip_file[91:]} ")

        if not path.exists(contributions_txt_file):
            zip_file = zipfile.ZipFile(contributions_zip_file, 'r')
            print('extracting %s' % contributions_zip_file)
            zip_file_contents = zip_file.namelist()
            for f in zip_file_contents:
                if ('.txt' in f):
                    zip_file.extract(f, path=CONTRIBUTIONS_PATH)
            zip_file.close()
        else:
            print(f"\tAlready downloaded file: {contributions_txt_file[91:]}")


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
            print(f"\tAlready downloaded file: {contributions_csv_file[91:]}")


class DatasetReader:
    """CSV helper to display summary statistics without reading the 1million+ row csv into memory."""

    def __init__(self, path):
        self.path = path
        self._length = None

    def __iter__(self):
        self._length = 0
        self._counter = Counter()
        with open(self.path, 'rU') as data:
            reader = csv.DictReader(data)
            for row in reader:
                self._length += 1

                yield row

    def __len__(self):
        if self._length is None:
            for row in self: continue

        return self._length

    @property
    def counter(self):
        """
        Gets the data for length and counter
        :return:
        """
        if self._counter is None:
            for row in self: continue
        return self._counter
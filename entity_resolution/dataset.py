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
from logging import info


class Dataset:
    """A helper to process the Illinois-campaign-contributions dataset."""

    SOURCE_URL = 'https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip'
    _file = 'Illinois-campaign-contributions'
    contributions_zip_file = _file + '.txt.zip'
    contributions_txt_file = _file + '.txt'
    contributions_csv_file = _file + '.csv'

    def __init__(self, PATH: str):
        self.PATH = PATH

    def download_zipfile(self):
        """Downloads Illinois-campaign-contributions.txt.zip.
        """
        # check if file already exists
        if not path.exists(self.PATH + Dataset.contributions_zip_file):
            info('downloading', self.PATH + Dataset.contributions_zip_file, '(~60mb) ...')
            u = requests.get(Dataset.SOURCE_URL)
            localFile = open(self.PATH + Dataset.contributions_zip_file, 'wb')
            localFile.write(u.content)
            localFile.close()
        else:
            info(f"\tAlready downloaded file: {self.PATH + Dataset.contributions_zip_file[91:]} ")

    def unzip_zipfile(self):
        """
        Unzips a zipfile to txt format.
        :return:
        """
        if not path.exists(self.PATH + Dataset.contributions_txt_file):
            zip_file = zipfile.ZipFile(self.PATH + Dataset.contributions_zip_file, 'r')
            info('extracting %s' % self.PATH + Dataset.contributions_zip_file)
            zip_file_contents = zip_file.namelist()
            for f in zip_file_contents:
                if (Dataset.contributions_txt_file in f):
                    zip_file.extract(f, path=self.PATH)
            zip_file.close()
        else:
            info(f"\tAlready downloaded file: {self.PATH + Dataset.contributions_txt_file[91:]}")


    def transform_txt_to_csv(self):
        """
        Converts the tab-delimited raw file to csv.
        :return:
        """
        if not path.exists(self.PATH + Dataset.contributions_csv_file):
            info('converting tab-delimited raw file to csv...')
            with open(self.PATH + Dataset.contributions_txt_file, 'rU') as txt_file, \
                    open(self.PATH + Dataset.contributions_csv_file, 'w') as csv_file:
                csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
                for line in txt_file:
                    if not all(ord(c) < 128 for c in line):
                        line = unidecode.unidecode(line)
                    row = line.rstrip('\t\r\n').split('\t')
                    if len(row) != 29:
                        info('skipping bad row (length %s, expected 29):' % len(row))
                        info(row)
                        continue
                    csv_writer.writerow(row)
        else:
            info(f"\tAlready downloaded file: {self.PATH + Dataset.contributions_csv_file[91:]}\n")


class DatasetReader:
    """CSV helper to display csv summary statistics."""
    _file = 'Illinois-campaign-contributions'
    contributions_csv_file = _file + '.csv'

    def __init__(self, PATH: str):
        self.PATH = PATH
        self._length = None

    def __iter__(self):
        self._length = 0
        self._counter = Counter()
        with open(self.PATH + DatasetReader.contributions_csv_file, 'r') as data: # 'rU'
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
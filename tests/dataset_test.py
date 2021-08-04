#!/usr/bin/env python3
from pytest import fixture
from entity_resolution.dataset import Dataset
from os import path
from entity_resolution.dataset import DatasetReader


ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'

@fixture
def my_dataset():
    return Dataset(PATH=ABS_PATH+EXTERNAL_DATA_DIR)

def test_source_url(my_dataset):
    assert my_dataset.SOURCE_URL == 'https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip'

def test_source_filename(my_dataset):
    assert my_dataset.contributions_zip_file == 'Illinois-campaign-contributions.txt.zip'



@fixture
def my_dataset_reader():
    return DatasetReader(PATH=ABS_PATH + '/external/' +EXTERNAL_DATA_DIR)

def test_dataset_len(my_dataset_reader):
    """

    :return:
    """
    expected = 1762975
    actual = my_dataset_reader.__len__()
    message = "Expected return value to be: {expected}"
    assert actual == expected, message
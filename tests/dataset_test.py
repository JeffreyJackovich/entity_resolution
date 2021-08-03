#!/usr/bin/env python3
from pytest import fixture
from entity_resolution.dataset import Dataset
from os import path

ABS_PATH = path.abspath('..')
EXTERNAL_DATA_DIR = '/data/external/'

@fixture
def my_dataset():
    return Dataset(PATH=ABS_PATH+EXTERNAL_DATA_DIR)

def test_source_url(my_dataset):
    assert my_dataset.SOURCE_URL == 'https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip'

def test_source_filename(my_dataset):
    assert my_dataset.contributions_zip_file == 'Illinois-campaign-contributions.txt.zip'


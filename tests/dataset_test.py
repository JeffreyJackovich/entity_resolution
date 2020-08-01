#!/usr/bin/env python3
from pytest import fixture
# from dataset import Dataset
from entity_resolution import dataset
from dataset import EXTERNAL_DATA_DIR


@fixture
def my_dataset():
    return dataset.Dataset()

def test_contributions_external_dir():
    assert EXTERNAL_DATA_DIR


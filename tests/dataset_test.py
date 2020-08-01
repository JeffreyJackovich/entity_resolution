#!/usr/bin/env python3
from pytest import fixture
from entity_resolution import dataset


@fixture
def my_dataset():
    return dataset.Dataset()

def test_contributions_external_dir():
    assert dataset.EXTERNAL_DATA_DIR


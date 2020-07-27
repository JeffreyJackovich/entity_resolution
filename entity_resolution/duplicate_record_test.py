#!/usr/bin/env python3
from pytest import fixture
# from mock import patch, Mock
from duplicate_record import DuplicateRecord


@fixture
def duplicate():
    return DuplicateRecord()

# def test_string_grouper(duplicate):
#     df = {'name': ['apple', 'apple', 'orange']}
#     assert duplicate.get_partial_duplicates(df)
#
#     return
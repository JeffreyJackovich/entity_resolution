#!/usr/bin/env python3
from pytest import fixture
# from mock import patch, Mock
from postgres import Postgres

from postgres import DATABASE_NAME
# DATABASE_NAME = getenv("POSTGRES_DB")

@fixture
def postgres():
    return Postgres()


# def test_environment_variables():
#     assert DATABASE_NAME
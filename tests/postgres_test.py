#!/usr/bin/env python3
from pytest import fixture
from entity_resolution.postgres import Postgres
from entity_resolution.postgres import DATABASE_SCHEMA_NAME


@fixture
def postgres():
    return Postgres()

def test_database_schema():
    assert DATABASE_SCHEMA_NAME



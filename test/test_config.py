#!/usr/bin/env python3
"""
You can auto-discover and run all tests with this command:

    py.test

Documentation: https://docs.pytest.org/en/latest/
"""
# import pytest

# def test_connection(monkeypatch):
#     """Patch the values of DEFAULT_CONFIG to specific testing values for this test"""
#     monkeypatch.setitem(config.DEFAULT_CONFIG, "DB", "test_postgres_db")
#     monkeypatch.setitem(config.DEFAULT_CONFIG, "USER", "test_user")
#
#     #expected result based on mocks
#     expected = "db_name=test_postgres_db; user_name=test_user;"
#
#     # test uses the monkeypatched dictionary settings
#     result = config.create_connection_string()
#     assert result == expected
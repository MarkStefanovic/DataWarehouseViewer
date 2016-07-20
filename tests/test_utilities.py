import os
import sqlite3

import pytest

from utilities import (
    cache,
    inspect_table,
    is_float,
    SqliteField,
    valid_sql_field_name
)


def setup_module(module):
    with sqlite3.connect('test.db') as con:
        con.execute("CREATE TABLE {t} ({f} INTEGER PRIMARY KEY)".format(t='test', f='id'))
        con.commit()


def teardown_module(module):
    os.remove('test.db')


def test_cache():
    @cache
    def test_func(a, b):
        return a + b
    test_func(3, 4) == 7
    test_func(3, 4) == 7
    test_func(b=4, a=3) == 7



def test_inspect_table():
    assert inspect_table(db='test.db', table='test') == [
        SqliteField(
            ix=0, name='id'
            , data_type='INTEGER'
            , nullable=False
            , default_value=None
            , primary_key=True
        )
    ]


def test_is_float():
    assert is_float('a') == False
    assert is_float(1.0) == True
    assert is_float(-1) == True
    assert is_float("'") == False


def test_valid_sql_field_name():
    assert valid_sql_field_name('0123_asdf') == False
    assert valid_sql_field_name('customer_id') == True
    assert valid_sql_field_name("'") == False
    assert valid_sql_field_name("_") == False

if __name__ == '__main__':
    pytest.main(__file__)
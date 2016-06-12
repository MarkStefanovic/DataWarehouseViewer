"""This module is responsible for creating the queries to send to the view to pull.

This is a string parser; it doesn't perform any IO.
"""
from collections import namedtuple, OrderedDict
import os
import sqlite3

from config import SimpleJsonConfig
from PyQt4 import QtCore

Field = namedtuple('Field', 'name, type, filterable')


class QueryManager(QtCore.QObject):
    """This class accepts criteria, compiles them, and returns a valid sql query."""
    query_error = QtCore.pyqtSignal(str)

    def __init__(self):
        super(QueryManager, self).__init__()
        cfg = SimpleJsonConfig()
        self._db = cfg.get_or_set_variable(key='db_path', default_value='test.db')
        self._table = cfg.get_or_set_variable(key='table', default_value='Customers')
        self._order_by = cfg.get_or_set_variable('order_by', 'FirstName')
        self._max_rows = cfg.get_or_set_variable('max_rows', 1000)
        self._fields = cfg.get_or_set_variable(
            'fields', [
                # ['CustomerID', 'CustomerID', 'int', False],
                ['FirstName', 'str', True],
                ['LastName', 'str', True],
                ['OrderDate', 'date', True],
                ['Amount', 'float', True],
            ]
        )
        # [DatabaseName, DisplayName, type, fiterable]
        self._field_types = cfg.field_types()
        self._criteria = {}

    def add_criteria(self, field_name: str, value: str, field_type: str) -> None:
        """Accept a string with a type and convert it into a where condition"""

        def date_end_wrapper():
            return "{} >= '{}'".format(field_name, value)

        def date_start_wrapper():
            return "{} <= '{}'".format(field_name, value)

        def float_wrapper():
            """Return where condition wrapper for field.

            Defaults to +/- 0.01
            """
            val = float(value)
            return '({name} >= {val} - 0.01 and {name} <= {val} + 0.01)'\
                .format(name=field_name, val=val)

        def int_wrapper():
            val = int(value)
            return "{} = {}".format(field_name, val)

        def str_wrapper():
            return "{} LIKE '%{}%'".format(field_name, value)

        wrappers = {
            'date_end': date_start_wrapper
            , 'date_start': date_end_wrapper
            , 'float': float_wrapper
            , 'int': int_wrapper
            , 'str': str_wrapper
        }
        # field_type = self._field_types[field_name]
        if field_type == 'date_start':
            key = field_name + '_start'
        elif field_type == 'date_end':
            key = field_name + '_end'
        else:
            key = field_name
        if value:
            self._criteria[key] = wrappers.get(field_type)()
        else:
            try:
                del self._criteria[key]
            except KeyError:
                pass

    def add_order_by(self, fieldname: str, asc_desc: str='asc') -> None:
        self._order_by = '{} {}'.format(fieldname, asc_desc)

    @property
    def fields(self):
        """Return a dictionary of Field tuples (name, type, filterable)."""

        return OrderedDict({
            i: Field(name=val[0], type=val[1], filterable=val[2])
            for i, val
            in enumerate(self._fields)
        })

    @property
    def filter_options(self):
        if self._fields:
            return sorted([(val.name, val.type) for val in self.fields.values()])
            # return {f[0]: f[1] for f in self.fields if f[2]}

    @property
    def headers(self):
        with sqlite3.connect(self._db) as con:
            cur = con.execute(self.sql)
            headers = [desc[0] for desc in cur.description]
        return headers

    @property
    def max_rows(self) -> str:
        if self._max_rows > 0:
            return "LIMIT " + str(self._max_rows)
        else:
            return ""

    @property
    def order_by(self) -> str:
        if self._order_by:
            return 'ORDER BY ' + self._order_by
        else:
            return ''

    def reset(self):
        self._criteria = {}

    def results(self) -> list:
        try:
            con_str = 'file:/' + os.path.abspath(self._db) + '?mode=ro'
            with sqlite3.connect(con_str, uri=True) as con:
                results = con.execute(self.sql).fetchall()

        #   clean up results, especially remove Nulls
            data = []
            for row, val in enumerate(results):
                data.append([])
                for i, col in enumerate(val):
                    if self.fields[i].type == 'float':
                        data[row].append(col or 0.0)
                    else:
                        data[row].append(col or '')
            return data
        except Exception as e:
            self.query_error.emit(str(e))
            return []

    @property
    def select_statement(self):
        fieldnames = [val[0] for val in self._fields]
        return "SELECT {fields} FROM {table}".format(fields=", ".join(fieldnames), table=self._table)

    @property
    def sql(self):
        return ' '.join((self.select_statement, self.where_clause, self.order_by, self.max_rows))

    @property
    def where_clause(self):
        if self._criteria:
            criteria = self._criteria.values()
            return 'WHERE ' + ' AND '.join(criteria)
        else:
            return ''


if __name__ == '__main__':
    qm = QueryManager()
    # qm.add_order_by('FirstName')
    # qm.add_criteria('FirstName', 'Mark')
    # qm.add_criteria('LastName', 'Stefanovic')
    print('sql:', qm.sql)
    print('results:', qm.results())
    print('headers:', qm.headers)
    print('filters:', qm.filter_options)
    print(qm.fields)
    print(qm.fields[0])
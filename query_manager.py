"""This module is responsible for procuring data for the model to prep and send to view.

FLOW OF INFORMATION
View (Input) -> Model -> Query Manager -> Exporter|Runner -> Data -> Query Manager -> Model -> View (Display):

No shortcuts allowed.
"""
from collections import namedtuple, OrderedDict
import os
import re

from json_config import SimpleJsonConfig
from PyQt4 import QtCore

from export_sql import ExportSql
from logger import log_error
from query_runner import QueryRunner

Field = namedtuple('Field', 'name, type, filterable')


class QueryManager(QtCore.QObject):
    """This class accepts criteria, compiles them, and returns a valid sql _query_manager."""

    error_signal = QtCore.pyqtSignal(str)
    exit_signal = QtCore.pyqtSignal()
    query_results_signal = QtCore.pyqtSignal(list)
    rows_returned_signal = QtCore.pyqtSignal(str)
    rows_exported_signal = QtCore.pyqtSignal(int)

    def __init__(self, config):
        super(QueryManager, self).__init__()
        cfg = SimpleJsonConfig(json_path=config)

        self._criteria = {}
        self._db = cfg.get_or_set_variable(key='db_path', default_value='test.db')
        self._exporter = ExportSql()
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
        self._table = cfg.get_or_set_variable(key='table', default_value='Customers')
        self._order_by = cfg.get_or_set_variable('order_by', '')
        self._max_export_rows = 500000  # TODO: change to config variable at app level
        self._max_rows = cfg.get_or_set_variable('max_rows', 1000)
        self._runner = QueryRunner()

    #   Connect Signals
        cfg.error_signal.connect(self.error_signal.emit)
        self._exporter.signals.error.connect(self.error_signal.emit)
        self._exporter.signals.rows_exported.connect(self.rows_exported_signal.emit)
        self.exit_signal.connect(self._exporter.signals.exit.emit)
        self.exit_signal.connect(self._runner.signals.exit.emit)
        self._runner.signals.results.connect(self.query_results_signal.emit)
        self._runner.signals.rows_returned_msg.connect(self.rows_returned_signal.emit)

    @log_error
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
            return '({name} >= {val} - 0.01 AND {name} <= {val} + 0.01)'\
                .format(name=field_name, val=value)

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

        if field_type == 'date_start':
            key = '{}_start'.format(field_name)
        elif field_type == 'date_end':
            key = '{}_end'.format(field_name)
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

    def export(self):
        self._exporter.start_pull(sql=self.sql_export, db_path=self._db)

    @property
    def fields(self) -> dict:
        """Return a dictionary of Field tuples (name, type, filterable)."""

        return OrderedDict({
            i: Field(name=val[0], type=val[1], filterable=val[2])
            for i, val
            in enumerate(self._fields)
        })

    def field_types(self) -> dict:
        if self._fields:
            return {val[0]: val[2] for val in self._fields}

    @property
    def filter_options(self):
        if self._fields:
            return sorted([(val.name, val.type) for val in self.fields.values() if val.filterable])

    @property
    def headers(self):
        return [fld.name for fld in self.fields.values()]

    @property
    def max_export_rows(self) -> str:
        if self._max_export_rows > 0:
            return "LIMIT {}".format(self._max_export_rows)
        else:
            return ""

    @property
    def max_rows(self) -> str:
        if self._max_rows > 0:
            return "LIMIT {}".format(self._max_rows)
        else:
            return ""

    @property
    def order_by(self) -> str:
        if self._order_by:
            return 'ORDER BY {}'.format(self._order_by)
        else:
            return ''

    @log_error
    def pull(self):
        try:
            self._runner.run_sql(
                query=self.sql
                , database_path=self._db
                , fields=self.fields
                , max_rows=self._max_rows
            )
        except Exception as e:
            err_msg = "Query execution error: {}".format(e)
            self.error_signal.emit(err_msg)

    def reset(self):
        self._criteria = {}

    @property
    def table(self):
        if re.match('.*[.]sql$', self._table): # table name is a path to a sql _query_manager
            fp = os.path.join('sql', self._table)
            with open(fp, 'r') as fh:
                qry = ' '.join([line.replace(r'\n', '') for line in fh.readlines()])
                return '({})'.format(qry)
        else:
            return self._table

    @property
    def select_statement(self):
        fieldnames = [val[0] for val in self._fields]
        return "SELECT {fields} FROM {table}".format(fields=", ".join(fieldnames), table=self.table)

    @property
    def sql(self):
        return ' '.join((self.select_statement, self.where_clause, self.order_by, self.max_rows))

    @property
    def sql_export(self):
        return ' '.join((self.select_statement, self.where_clause, self.order_by, self.max_export_rows))

    @property
    def str_criteria(self) -> str:
        if self._criteria:
            return '(' + '; '.join([criteria for criteria in self._criteria.values()]) + ')'
        return 'top {} rows'.format(self._max_rows)

    @property
    def where_clause(self):
        if self._criteria:
            criteria = self._criteria.values()
            return 'WHERE ' + ' AND '.join(criteria)
        else:
            return ''

def iterrows(cursor, chunksize=1000, max_rows=1000):
    """An iterator over dataset to minimize memory usage."""
    rows = 0
    while rows <= max_rows:
        rows += chunksize
        results = cursor.fetchmany(chunksize)
        if not results:
            break
        for result in results:
            yield result


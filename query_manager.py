"""This module is responsible for creating the queries to send to the view to pull.

The QueryManager class is essentially the Controller in an MVC paradigm.  It only
talks to the model, and the model relays the information to the view.
"""
from collections import namedtuple, OrderedDict
# from concurrent.futures import ThreadPoolExecutor
import os
import sqlite3

from config import SimpleJsonConfig
from PyQt4 import QtCore

from messenger import global_message_queue

Field = namedtuple('Field', 'name, type, filterable')

class QueryManager(QtCore.QObject):
    """This class accepts criteria, compiles them, and returns a valid sql query."""
    query_error = QtCore.pyqtSignal(str)
    query_results = QtCore.pyqtSignal(list)

    def __init__(self):
        super(QueryManager, self).__init__()
        cfg = SimpleJsonConfig()
        cfg.config_errored.connect(self.configuration_error)
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
        self._qry_pool = QueryThreadPool()

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
            return '(NULLIF({name}, 0) >= {val} - 0.01 and NULLIF({name}, 0) <= {val} + 0.01)'\
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
        # field_type = self._field_types[field_name]
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

    @QtCore.pyqtSlot(str)
    def configuration_error(self, msg) -> None:
        self.query_error.emit(msg)
        # simply pass the message along
        # We don't want a race to display the configuration and sql errors
        #   to the view.

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
            return sorted([(val.name, val.type) for val in self.fields.values() if val.filterable])

    @property
    def headers(self):
        return [fld.name for fld in self.fields.values()]

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

    def pull(self):
        try:
            new_thread = QueryRunner(
                query=self.sql
                , database_path=self._db
                , fields=self.fields
                , max_rows=self._max_rows)
            new_thread.results_pulled.connect(self.got_results)
            new_thread.query_errored.connect(self.query_thread_errored)
            self._qry_pool.add_thread(new_thread)
            # self._qry_pool.start()
            new_thread.start()
        except Exception as e:
            self.query_error.emit(str(e))

    def reset(self):
        self._criteria = {}

    @QtCore.pyqtSlot(str)
    def got_results(self, query_results):
        self.query_results.emit(query_results)

    @QtCore.pyqtSlot(str)
    def query_thread_errored(self, error_msg):
        self.query_error.emit(error_msg)

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

class QueryRunner(QtCore.QThread):
    results_pulled = QtCore.pyqtSignal(list)
    query_errored = QtCore.pyqtSignal(str)
    rows_returned = QtCore.pyqtSignal(int)

    def __init__(self, query, database_path, fields, max_rows=1000):
        super(QueryRunner, self).__init__()
        self._qry = query
        self._db = database_path
        self._max_rows = max_rows
        self._fields = fields

    def run(self):
        try:
            results = []
            con_str = 'file:/{}?mode=ro'.format(os.path.abspath(self._db))
            with sqlite3.connect(con_str, uri=True) as con:
                cursor = con.cursor()
                cursor.execute(self._qry)
                for result in iterrows(cursor, chunksize=1000, max_rows=self._max_rows):
                    results.append(list(result))
            self.process_results(results)
        except Exception as e:
            err_msg = 'Query execution error: {}'.format(e)
            self.query_errored.emit(err_msg)

    def process_results(self, results):
        try:
            for row, val in enumerate(results):
                for i, col in enumerate(val):
                    if self._fields[i].type == 'float':
                        results[row][i] = col or 0.0
                    else:
                        results[row][i] = col or ''
            global_message_queue.rows_returned_signal.emit(len(results))
            self.results_pulled.emit(results)
        except Exception as e:
            err_msg = 'Error processing query results: {}'.format(e)
            self.query_errored.emit(err_msg)

    def stop(self):
        # self.terminate()
        self.exit()

class QueryThreadPool(QtCore.QObject):
    def __init__(self):
        super(QueryThreadPool, self).__init__()
        self.pool = QtCore.QThreadPool()
        self.pool.setMaxThreadCount(1)
        self.threads = []
        global_message_queue.exit_signal.connect(self.stop)

    def add_thread(self, thread):
        self.stop()
        self.threads.append(thread)

    def start(self):
        # for thread in self.threads.pop():
        #     self.pool.start(thread)
        thread = self.threads.pop()
        self.pool.start(thread)

    def stop(self):
        for i in range(len(self.threads)):
            self.threads.pop().stop()

        # for thread in self.threads:
        #     thread.stop()
        # self.threads = []

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
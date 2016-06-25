import os
from PyQt4 import QtCore
import sqlite3
import time

from logger import log_error
from utilities import iterrows


class QueryRunnerSignals(QtCore.QObject):
    error = QtCore.pyqtSignal(str)
    exit = QtCore.pyqtSignal()
    done = QtCore.pyqtSignal()
    results = QtCore.pyqtSignal(list)
    rows_returned_msg = QtCore.pyqtSignal(str)


class QueryRunnerThread(QtCore.QThread):

    def __init__(self, query, database_path, fields, max_rows=1000):
        super(QueryRunnerThread, self).__init__()
        self._qry = query
        self._db = database_path
        self._max_rows = max_rows
        self._fields = fields
        self.signals = QueryRunnerSignals()
        self._start_time = time.time()
        self.stop_everything = False

    @log_error
    def pull(self):
        try:
            results = []
            con_str = 'file:/{}?mode=ro'.format(os.path.abspath(self._db))
            with sqlite3.connect(con_str, uri=True) as con:
                cursor = con.cursor()
                cursor.execute(self._qry)
                for result in iterrows(cursor, chunksize=1000):
                    if self.stop_everything: return
                    results.append(list(result))
            if self.stop_everything: return
            self.process_results(results)
        except Exception as e:
            err_msg = 'Query execution error: {}'.format(e)
            self.signals.error.emit(err_msg)

    @log_error
    def process_results(self, results):
        try:
            for row, val in enumerate(results):
                for i, col in enumerate(val):
                    if self._fields[i].type == 'float':
                        results[row][i] = col or 0.0
                    else:
                        results[row][i] = col or ''
            return_msg = '{} rows returned in {} seconds'.format(len(results), int(time.time() - self._start_time))
            self.signals.rows_returned_msg.emit(return_msg)
            self.signals.results.emit(results)
        except Exception as e:
            err_msg = "Error exporting _query_manager results: {}".format(e)
            self.signals.error.emit(err_msg)

    def run(self):
        self.pull()

    def stop(self):
        self.stop_everything = True
        self.exit()
        self.quit()


class QueryRunner(QtCore.QObject):
    """This class manages the currently active ExportSql thread"""
    def __init__(self):
        super(QueryRunner, self).__init__()
        self.signals = QueryRunnerSignals()
        self.thread = None

    @log_error
    def run_sql(self, query, database_path, fields, max_rows):
        self.signals.exit.emit()  # stop current thread
        self.thread = QueryRunnerThread(query, database_path, fields, max_rows)
        self.signals.exit.connect(self.thread.stop)
        self.thread.signals.error.connect(self.signals.error.emit)
        self.thread.signals.rows_returned_msg.connect(self.signals.rows_returned_msg.emit)
        self.thread.signals.results.connect(self.signals.results.emit)
        self.thread.start()


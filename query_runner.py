from PyQt4 import QtCore
import time

from sqlalchemy.sql import Select

from star_schema.db import fetch
from logger import log_error


class QueryRunnerSignals(QtCore.QObject):
    error = QtCore.pyqtSignal(str)
    exit = QtCore.pyqtSignal()
    done = QtCore.pyqtSignal()
    results = QtCore.pyqtSignal(list)
    rows_returned_msg = QtCore.pyqtSignal(str)


class QueryRunnerThread(QtCore.QThread):

    def __init__(self,
        query: Select,
        show_rows_returned: bool=True
    ) -> None:

        super(QueryRunnerThread, self).__init__()
        self.query = query  # type: Select
        self.signals = QueryRunnerSignals()
        self.start_time = time.time()
        self.show_rows_returned = show_rows_returned

    @log_error
    def pull(self) -> None:
        try:
            results = fetch(self.query)
            if self.show_rows_returned:
                self.signals.rows_returned_msg.emit(
                    '{} rows returned in {} seconds'.format(
                        len(results),
                        int(time.time() - self.start_time)
                    )
                )
            self.signals.results.emit(results)
        except Exception as e:
            self.signals.error.emit(
                'Query execution error: {err}; {qry}'.format(
                    err=e
                    , qry=self.query
                )
            )

    def run(self) -> None:
        self.pull()

    def stop(self) -> None:
        self.exit()
        self.quit()


class QueryRunner(QtCore.QObject):
    """This class manages the currently active ExportSql thread"""
    def __init__(self) -> None:
        super(QueryRunner, self).__init__()
        self.signals = QueryRunnerSignals()
        self.thread = None

    @log_error
    def run_sql(self, query: str, show_rows_returned: bool=True) -> None:
        self.signals.exit.emit()  # stop current thread
        self.thread = QueryRunnerThread(query, show_rows_returned)
        self.signals.exit.connect(self.thread.stop)
        self.thread.signals.error.connect(self.signals.error.emit)
        self.thread.signals.rows_returned_msg.connect(self.signals.rows_returned_msg.emit)
        self.thread.signals.results.connect(self.signals.results.emit)
        self.thread.start()


import logging
import time

from PyQt4 import QtCore
from sqlalchemy.sql import Select
from typing import Optional, Callable, List, Any

from star_schema.db import fetch

module_logger = logging.getLogger('app')


class QueryRunnerSignals(QtCore.QObject):
    error = QtCore.pyqtSignal(str)
    exit = QtCore.pyqtSignal()
    done = QtCore.pyqtSignal()
    results = QtCore.pyqtSignal(list)
    rows_returned_msg = QtCore.pyqtSignal(str)


class QueryRunnerThread(QtCore.QThread):
    def __init__(self, *,
        query: Select,
        con_str: str,
        show_rows_returned: bool=True,
        callback: Optional[Callable[[Any], None]]=None
    ) -> None:

        self.logger = module_logger.getChild('QueryRunnerThread')
        super().__init__()
        self.query = query  # type: Select
        self.signals = QueryRunnerSignals()
        self.start_time = time.time()
        self.show_rows_returned = show_rows_returned
        self.con_str = con_str
        self.callback = callback

    def pull(self) -> None:
        try:
            results = fetch(qry=self.query, con_str=self.con_str)
            if self.show_rows_returned:
                err_msg = '{} rows returned in {} seconds' \
                    .format(len(results), int(time.time() - self.start_time))
                self.logger.debug('pull: {}'.format(err_msg))
                self.signals.rows_returned_msg.emit(err_msg)
            self.signals.results.emit(results)
            if self.callback:
                self.callback(results)
        except Exception as e:
            err_msg = 'Query execution error: {err}; {qry}' \
                .format(err=e, qry=self.query)
            self.logger.debug('pull: {}'.format(err_msg))
            self.signals.error.emit(err_msg)

    def run(self) -> None:
        self.pull()

    def stop(self) -> None:
        self.exit()
        self.quit()


class QueryRunner(QtCore.QObject):
    """This class manages the currently active ExportSql thread"""
    def __init__(self) -> None:
        super().__init__()
        self.signals = QueryRunnerSignals()
        self.thread = None

    def run_sql(self, *,
            query: Select,
            con_str: str,
            show_rows_returned: bool=True,
            callback: Optional[Callable[[Any], None]]=None
        ) -> None:
        self.signals.exit.emit()  # stop current thread
        self.thread = QueryRunnerThread(
            query=query,
            con_str=con_str,
            show_rows_returned=show_rows_returned,
            callback=callback
        )
        self.signals.exit.connect(self.thread.stop)
        self.thread.signals.error.connect(self.signals.error.emit)
        self.thread.signals.rows_returned_msg.connect(self.signals.rows_returned_msg.emit)
        self.thread.signals.results.connect(self.signals.results.emit)
        self.thread.start()


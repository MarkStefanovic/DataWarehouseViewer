import os
from subprocess import Popen
from typing import List

from PyQt4 import QtCore
import xlwt

from logger import log_error
from db import iterrows


class SqlSignals(QtCore.QObject):
    error = QtCore.pyqtSignal(str)
    exit = QtCore.pyqtSignal()
    rows_exported = QtCore.pyqtSignal(int)
    done = QtCore.pyqtSignal()


class QueryExporter(QtCore.QObject):
    """This class manages the currently active ExportSql thread"""

    def __init__(self) -> None:
        super(QueryExporter, self).__init__()
        self.signals = SqlSignals()
        self.thread = None  # type: ExportSqlThread

    def start_pull(self, query, headers: List[str]) -> None:
        self.signals.exit.emit()  # stop current thread
        self.thread = ExportSqlThread(query, headers)
        self.signals.exit.connect(self.thread.stop)
        self.thread.signals.error.connect(self.signals.error.emit)  # pass along
        self.thread.signals.rows_exported.connect(self.signals.rows_exported.emit)  # pass along
        self.thread.start()


class ExportSqlThread(QtCore.QThread):
    """
     Writes a sql query_manager to an Excel workbook.
    """
    def __init__(self, query, headers) -> None:
        super(ExportSqlThread, self).__init__()
        self.query = query
        self.headers = headers
        self.signals = SqlSignals()
        self.stop_everything = False
        #   stop thread in relatively save spots

    @log_error
    def run(self) -> None:
        try:
            folder = 'output'
            if not os.path.exists(folder) or not os.path.isdir(folder):
                os.mkdir(folder)
            output_path = os.path.join(folder, 'temp.xls')

            wb = xlwt.Workbook()
            sht = wb.add_sheet('temp', cell_overwrite_ok=True)
            header_style = xlwt.easyxf(
                'pattern: pattern solid, fore_colour dark_blue;'
                'font: colour white, bold True;'
            )
            for i, x in enumerate(self.headers):
                sht.write(0, i, x, header_style)

            n = 0
            if self.stop_everything: return
            try:
                for row in iterrows(self.query):
                    if self.stop_everything: return
                    n += 1
                    for i, val in enumerate(row):
                        if val:
                            sht.write(n, i, str(val))
                    if n % 1000 == 0:
                        self.signals.rows_exported.emit(n)
            except:
                pass
            self.signals.rows_exported.emit(n)
            if self.stop_everything: return
            wb.save(output_path)
            Popen(output_path, shell=True)
        except Exception as e:
            err_msg = "Error exporting query_manager results: {err}; {qry}"\
                .format(err=e, qry=self.query)
            self.signals.error.emit(err_msg)

    def stop(self) -> None:
        self.stop_everything = True
        self.exit()
        self.quit()




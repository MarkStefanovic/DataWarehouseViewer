import os
import time
from subprocess import Popen
from typing import List

from PyQt4 import QtCore
import xlwt

from logger import log_error
from db import iterrows
from utilities import delete_old_outputs


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

    def start_export(self, rows: List[List[str]], header: List[str]) -> None:
        self.signals.exit.emit()  # stop current thread
        self.thread = ExportSqlThread(rows, header)
        self.signals.exit.connect(self.thread.stop)
        self.thread.signals.error.connect(self.signals.error.emit)  # pass along
        self.thread.signals.rows_exported.connect(self.signals.rows_exported.emit)  # pass along
        self.thread.start()


class ExportSqlThread(QtCore.QThread):
    """
     Writes a sql query_manager to an Excel workbook.
    """
    def __init__(self, rows: List[List[str]], header: List[str]) -> None:
        super(ExportSqlThread, self).__init__()
        self.rows = rows  # type: List[List[str]]
        self.header = header  # type: List[str]
        self.signals = SqlSignals()
        # stop thread in relatively safe spots
        self.stop_everything = False

    @log_error
    def run(self) -> None:
        try:
            folder = 'output'
            if not os.path.exists(folder) or not os.path.isdir(folder):
                os.mkdir(folder)

            wb = xlwt.Workbook()
            sht = wb.add_sheet('temp', cell_overwrite_ok=True)
            header_style = xlwt.easyxf(
                'pattern: pattern solid, fore_colour dark_blue;'
                'font: colour white, bold True;'
            )
            for i, x in enumerate(self.header):
                sht.write(0, i, x, header_style)

            n = 0
            if self.stop_everything: return
            try:
                for row in self.rows:
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
            if self.stop_everything:
                return
            t = time.strftime("%Y-%m-%d.%H%M%S")
            dest = os.path.join(folder, 'temp_{}.xls'.format(t))
            delete_old_outputs(folder)
            wb.save(dest)
            Popen(dest, shell=True)
        except Exception as e:
            err_msg = "Error exporting query_manager results: {err}; {qry}"\
                .format(err=e, qry=self.rows)
            self.signals.error.emit(err_msg)

    def stop(self) -> None:
        self.stop_everything = True
        self.exit()
        self.quit()




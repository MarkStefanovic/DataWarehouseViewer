import os
import sqlite3
from subprocess import Popen

from PyQt4 import QtCore
import xlwt

from utilities import iterrows


class ExitException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self, *args, **kwargs)


class SqlSignals(QtCore.QObject):
    error = QtCore.pyqtSignal(str)
    exit = QtCore.pyqtSignal()
    rows_exported = QtCore.pyqtSignal(int)
    done = QtCore.pyqtSignal()


class ExportSql(QtCore.QObject):

    def __init__(self):
        super(ExportSql, self).__init__()
        self.signals = SqlSignals()
        self.thread = None

    def start_pull(self, sql, db_path):
        self.signals.exit.emit()  # stop current thread
        self.thread = ExportSqlThread(sql, db_path)
        self.signals.exit.connect(self.thread.stop)
        self.thread.signals.error.connect(self.signals.error.emit)  # pass along
        self.thread.signals.rows_exported.connect(self.signals.rows_exported.emit)  # pass along
        self.thread.start()


class ExportSqlThread(QtCore.QThread):
    """
     Writes a sql _query to an Excel workbook.
    """
    def __init__(self, sql, db_path):
        super(ExportSqlThread, self).__init__()
        self._sql = sql
        self._db_path = db_path
        self.signals = SqlSignals()
        self.stop_everything = False
        #   stop thread in relatively save spots

    def run(self):
        try:
            folder = 'output'
            if not os.path.exists(folder) or not os.path.isdir(folder):
                os.mkdir(folder)
            output_path = os.path.join(folder, 'temp.xls')

            con_str = 'file:/{}?mode=ro'.format(os.path.abspath(self._db_path))
            with sqlite3.connect(con_str, uri=True) as con:
                cursor = con.cursor()
                cursor.execute(self._sql)
                wb = xlwt.Workbook()
                sht = wb.add_sheet('temp', cell_overwrite_ok=True)
                header_style = xlwt.easyxf(
                    'pattern: pattern solid, fore_colour dark_blue;'
                    'font: colour white, bold True;'
                )
                headers = [cn[0] for cn in cursor.description]
                [sht.write(0, i, x, header_style) for i, x in enumerate(headers)]
                n = 0
                if self.stop_everything: return
                for result in iterrows(cursor=cursor, chunksize=1000):
                    if self.stop_everything: return
                    n += 1
                    for i, val in enumerate(result):
                        sht.write(n, i, val)
                    if n % 1000 == 0:
                        self.signals.rows_exported.emit(n)
                self.signals.rows_exported.emit(n)
            if self.stop_everything: return
            wb.save(output_path)
            Popen(output_path, shell=True)
        except Exception as e:
            err_msg = "Error exporting _query results: {}".format(e)
            self.signals.error.emit(err_msg)

    def stop(self):
        self.stop_everything = True
        self.exit()
        self.quit()




"""This module is responsible for procuring data for the model to prep and send to view.

"""
from typing import Dict, List

from PyQt4 import QtCore

from config import cfg
from db import Transaction
from query_exporter import QueryExporter
from logger import log_error
from query_runner import QueryRunner
from schema import Table
from sqlalchemy import Table
from utilities import immutable_property


class QueryManager(QtCore.QObject):
    """Create a query from user input."""

    error_signal = QtCore.pyqtSignal(str)
    exit_signal = QtCore.pyqtSignal()
    query_results_signal = QtCore.pyqtSignal(list)
    row_updated_signal = QtCore.pyqtSignal(str, tuple, int)
    rows_returned_signal = QtCore.pyqtSignal(str)
    rows_exported_signal = QtCore.pyqtSignal(int)

    def __init__(self, table: Table) -> None:
        super(QueryManager, self).__init__()

        self.exporter = QueryExporter()
        self.runner = QueryRunner()
        self.table = table

    #   Connect Signals
        self.exporter.signals.error.connect(self.error_signal.emit)
        self.exporter.signals.error.connect(self.error_signal.emit)
        self.exporter.signals.rows_exported.connect(self.rows_exported_signal.emit)
        self.exit_signal.connect(self.exporter.signals.exit.emit)
        self.exit_signal.connect(self.runner.signals.exit.emit)
        self.runner.signals.error.connect(self.error_signal.emit)
        self.runner.signals.results.connect(self.process_results)
        self.runner.signals.rows_returned_msg.connect(self.rows_returned_signal.emit)

    @log_error
    def add_criteria(self, filter_ix: int, value: str) -> None:
        """Accept a string with a type and convert it into a where condition"""
        self.table.filters[filter_ix].set_value(value)

    @immutable_property
    def editable_fields_indices(self) -> List[int]:
        if self.table.editable:
            return [
                i for i, fld
                in enumerate(self.table.fields)
                if i != self.table.primary_key_index
            ]
        return []

    def get_field_index(self, name: str) -> int:
        return min(
            i for i, fld
            in enumerate(self.table.fields)
            if fld.name == name
        )

    def export(self) -> None:
        self.exporter.start_pull(query=self.sql_export, headers=self.headers)

    @immutable_property
    def headers(self) -> List[str]:
        """Return a list of field headers for display."""
        return [fld.display_name for fld in self.table.fields]

    @log_error
    def pull(self) -> None:
        self.runner.run_sql(query=self.sql_display)

    @log_error
    @QtCore.pyqtSlot(list)
    def process_results(self, results: list) -> None:
        """Convert data to specified data types"""
        processed = []
        try:
            for r, row in enumerate(results):
                processed.append(list(row))
                for c, col in enumerate(row):
                    processed[r][c] = self.table.fields[c].dtype.convert(col)
            self.query_results_signal.emit(processed)
        except Exception as e:
            err_msg = "Error processing results: {}".format(e)
            self.error_signal.emit(err_msg)

    def reset(self) -> None:
        for f in self.table.filters:
            f.set_value(value='')

    @log_error
    def save_changes(self, changes: Dict[str, List[tuple]]) -> Dict[str, int]:
        """Persist a change to the database."""

        trans = Transaction()
        try:
            for row in changes['deleted']:
                trans.execute(self.table.delete_row(row[self.table.primary_key_index]))

            for row in changes['added']:
                trans.execute(self.table.add_row(values=list(row)))

            for row in changes['updated']:
                id = row[self.table.primary_key_index]
                trans.execute(self.table.update_row(id=id, values=list(row)))

            return trans.commit()

        except:
            raise

    @property
    def sql_display(self) -> str:
        return self.table.select(max_rows=cfg.app.maximum_display_rows)

    @property
    def sql_export(self) -> str:
        return self.table.select(max_rows=cfg.app.maximum_export_rows)



# if __name__ == "__main__":
#     import doctest
#     doctest.testmod()
#
#     from PyQt4.QtGui import QApplication
#     import sys
#     a = QApplication(sys.argv)
#
#
#     from sqlalchemy.dialects import postgresql
#     tbl = cfg.tables[0]
#     q = QueryManager(tbl)
#     print(q.table.filters)
#     print(q.foreign_keys)
#     print(q.sql_display.compile(dialect=postgresql.dialect()))
#     q.pull()
#
#     a.exec_()
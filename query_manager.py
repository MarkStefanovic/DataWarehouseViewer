"""This module is responsible for procuring data for the model to prep and send to view.

"""
from sqlalchemy.sql import Select
from typing import Dict, List, Tuple

from PyQt4 import QtCore

from config import cfg
from db import Transaction
from query_exporter import QueryExporter
from logger import log_error
from query_runner import QueryRunner
from schema import Fact, Table
from sqlalchemy import Table
from utilities import static_property


class QueryManager(QtCore.QObject):
    """Create a rows from user input."""

    error_signal = QtCore.pyqtSignal(str)
    query_results_signal = QtCore.pyqtSignal(list)

    def __init__(self, table: Table) -> None:
        super(QueryManager, self).__init__()

        self.exporter = QueryExporter()
        self.runner = QueryRunner()
        self.table = table
        self.star = cfg.star(self.table.table_name) if isinstance(self.table, Fact) else None
        self.filters = self.star.filters if self.star else self.table.filters
        self.headers = [fld.display_name for fld in self.table.fields]

    #   Connect Signals
        self.runner.signals.results.connect(self.process_results)

    def add_criteria(self, filter_ix: int, value: str) -> None:
        """Accept a string with a type and convert it into a where condition"""
        self.filters[filter_ix].value = value

    @static_property
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

    def export(self, rows: List[List[str]], header: List[str]) -> None:
        self.exporter.start_export(rows=rows, header=header)

    def pull(self) -> None:
        self.runner.run_sql(query=self.sql_display)

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
            f.value = ''

    @log_error
    def save_changes(self, changes: Dict[str, List[tuple]]) -> Dict[str, int]:
        """Persist a change to the database."""

        trans = Transaction()
        new_rows_id_map = []  # type: List[Tuple[int, int]]
        try:
            for row in changes['deleted']:
                trans.execute(self.table.delete_row(row[self.table.primary_key_index]))

            for row in changes['added']:
                id = row[self.table.primary_key_index]
                new_id = trans.execute(self.table.add_row(values=list(row)))
                new_rows_id_map.append((id, new_id))

            for row in changes['updated']:
                id = row[self.table.primary_key_index]
                trans.execute(self.table.update_row(pk=id, values=list(row)))

            results = trans.commit()
            results['new_rows_id_map'] = new_rows_id_map
            return results

        except:
            raise

    @property
    def sql_display(self) -> Select:
        if self.star:
            return self.star.select
        return self.table.select

    @property
    def sql_export(self) -> Select:
        if self.star:
            return self.star.select
        return self.table.select

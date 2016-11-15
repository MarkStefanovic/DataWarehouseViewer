"""This module is responsible for procuring data for the model to prep and send to view.

"""
from sqlalchemy.sql import Select
from typing import Dict, List, Tuple, Optional

from PyQt4 import QtCore

from star_schema.config import cfg
from star_schema.custom_types import TableName
from star_schema.db import Transaction
from query_exporter import QueryExporter
from logger import log_error, rotating_log
from query_runner import QueryRunner
from star_schema.constellation import (
    Fact,
    Field,
    Filter,
    Table,
    View
)
from star_schema.constellation import convert_value

from star_schema.utilities import static_property


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
        self.view = cfg.view(self.table.display_name) if isinstance(self.table, View) else None

        self.filters = self.star.filters if self.star else self.table.filters
        self.logger = rotating_log('query_manager.QueryManager')

    #   Connect Signals
        self.runner.signals.results.connect(self.process_results)

    def add_criteria(self, filter_ix: int, value: str) -> None:
        """Accept a string with a type and convert it into a where condition"""
        self.filters[filter_ix].value = value

    @static_property
    def base(self):
        if self.view:
            return self.view
        elif self.star:
            return self.star
        else:
            return self.table

    @static_property
    def editable_fields_indices(self) -> List[int]:
        if self.table.editable:
            return [
                i for i, fld
                in enumerate(self.fields)
                if i != self.table.primary_key_index
                    and self.fields[i].editable
            ]
        return []

    def get_field_index(self, name: str) -> int:
        return min(
            i for i, fld
            in enumerate(self.fields)
            if fld.name == name
        )

    @static_property
    def headers(self):
        return [fld.display_name for fld in self.fields]

    def export(self, *,
            rows: List[List[str]],
            header: List[str],
            table_name: TableName
    ) -> None:
        self.exporter.start_export(rows=rows, header=header, table_name=table_name)

    @static_property
    def fields(self) -> List[Field]:
        return self.base.fields

    @static_property
    def filters_by_name(self) -> Optional[Dict[str, Filter]]:
        return {
            flt.display_name: flt
            for flt in self.base.filters
        }

    def pull(self, show_rows_returned: bool=True) -> None:
        self.runner.run_sql(
            query=self.sql_display,
            show_rows_returned=show_rows_returned
        )

    @QtCore.pyqtSlot(list)
    def process_results(self, results: list) -> None:
        """Convert data to specified data types"""
        processed = []
        try:
            for r, row in enumerate(results):
                processed.append(list(row))
                for c, col in enumerate(row):
                    try:
                        field_type = self.fields[c].dtype
                        processed[r][c] = convert_value(
                            field_type=field_type,
                            value=col
                        )
                    except Exception as e:
                        print('Error converting value {}, row {}, col {}, err {}'
                              .format(col, r, c, str(e)))
                        processed[r][c] = col
            self.query_results_signal.emit(processed)
        except Exception as e:
            err_msg = "Error processing results: {}".format(e)
            self.logger.debug('process_results: {}'.format(err_msg))
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
        elif self.view:
            return self.view.select
        return self.table.select


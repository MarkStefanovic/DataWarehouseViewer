"""This module is responsible for procuring data for the model to prep and send to view.

"""
import logging

from sqlalchemy.sql import Select
from typing import Dict, List, Tuple, Optional

from PyQt4 import QtCore

# from star_schema.config import cfg
from star_schema.custom_types import TableName, ColumnIndex, FieldName
from star_schema.db import Transaction
from query_exporter import QueryExporter
from query_runner import QueryRunner
from star_schema.constellation import (
    Fact,
    Filter,
    DisplayPackage
)
from star_schema.constellation import convert_value

from star_schema.utilities import static_property

module_logger = logging.getLogger('app.' + __name__)


class QueryManager(QtCore.QObject):
    """Create a rows from user input."""

    error_signal = QtCore.pyqtSignal(str)
    query_results_signal = QtCore.pyqtSignal(list)

    def __init__(self, config: DisplayPackage) -> None:
        super(QueryManager, self).__init__()
        self.logger = module_logger.getChild('QueryManager')
        self.config = config
        self.exporter = QueryExporter()
        self.runner = QueryRunner()
        self.table = config.table
        self.base = config.display_base

    #   Connect Signals
        self.runner.signals.results.connect(self.process_results)

    def add_criteria(self, filter_ix: int, value: str) -> None:
        """Accept a string with a type and convert it into a where condition"""
        self.filters[filter_ix].value = value

    @property
    def filters(self):
        if isinstance(self.table, Fact):
            return self.base.filters
        return self.table.filters

    def export(self, *,
            rows: List[List[str]],
            header: List[str],
            table_name: TableName
    ) -> None:
        self.exporter.start_export(
            rows=rows,
            header=header,
            table_name=table_name
        )

    @static_property
    def filters_by_name(self) -> Optional[Dict[str, Filter]]:
        return {
            flt.display_name: flt
            for flt in self.base.filters
        }

    def pull(self, show_rows_returned: bool=True) -> None:
        try:
            self.runner.run_sql(
                query=self.sql_display,
                show_rows_returned=show_rows_returned,
                con_str=self.config.app.db_path
            )
        except AttributeError as e:
            err_msg = 'pull: {} does not a valid sql statement associated ' \
                      'with it; error: {}'.format(self.table.display_name, str(e))
            self.logger.error(err_msg)
            raise
        except Exception as e:
            err_msg = "Error compising the query for table {}; error: {}" \
                      .format(self.base.display_name, str(e))
            self.logger.error('pull: {}'.format(err_msg))
            raise

    # @static_property
    # def column_index_mapping(self) -> Dict[ColumnIndex, ColumnIndex]:
    #     # we may want to change our method for looking up the select statement
    #     # column order, since sqla could change it's __str__ implementation one day
    #     qry_field_order = [str(fld) for fld in self.sql_display.columns]
    #
    #     print('field order', self.config.field_order.keys())
    #     return qry_field_order

    @QtCore.pyqtSlot(list)
    def process_results(self, results: list) -> None:
        """Convert data to specified data types"""
        processed = []
        try:
            for r, row in enumerate(results):
                processed.append(list(row))
                for c, col in enumerate(row):
                    try:
                        field_type = self.config.field_order[c].dtype
                        processed[r][c] = convert_value(
                            field_type=field_type,
                            value=col
                        )
                    except Exception as e:
                        self.logger.debug(
                            'process_results: Error converting value {}, '
                            'row {}, col {}, err {}'.format(col, r, c, str(e))
                        )
                        processed[r][c] = col
            self.query_results_signal.emit(processed)
        except Exception as e:
            err_msg = "Error processing results: {}".format(e)
            self.logger.debug('process_results: {}'.format(err_msg))
            self.error_signal.emit(err_msg)

    def reset(self) -> None:
        for f in self.table.filters:
            f.value = ''

    def save_changes(self, changes: Dict[str, List[tuple]]) -> Dict[str, int]:
        """Persist a change to the database.

        :param changes: A dict keyed by the change type ('added', 'updated',
                 'deleted') and valued by the list of tuples representing rows
                 with changes of that type.
        :return: Returns a dict indexed by the change type ('added', 'updated',
                 'deleted') and valued by the count of said changes that were
                 successfully saved to the database.
        """
        trans = Transaction(con_str=self.config.app.db_path)
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
        except Exception as e:
            self.logger.error(
                'save_changes: Unable to save changes; error {}'
                .format(str(e))
            )
            raise

    @property
    def sql_display(self) -> Select:
        return self.base.select


import logging
import uuid
from copy import deepcopy
from functools import partial

from PyQt4 import QtCore
from PyQt4.QtCore import QModelIndex
from sortedcontainers import SortedSet
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Set
)

from query_exporter import QueryExporter
from query_runner import QueryRunner
from star_schema.constellation import (
    convert_value,
    DisplayPackage,
    format_value
)
from star_schema.custom_types import (
    ColumnIndex,
    SqlDataType,
    FieldDisplayName
)
from star_schema.custom_types import FieldType

module_logger = logging.getLogger('app.' + __name__)


class AbstractModel(QtCore.QAbstractTableModel):
    filters_changed_signal = QtCore.pyqtSignal()
    error_signal = QtCore.pyqtSignal(str)
    query_results_signal = QtCore.pyqtSignal(list)

    def __init__(self, config: DisplayPackage) -> None:

        super().__init__()
        self.logger = module_logger.getChild('AbstractModel')
        self.config = config

        self.query_runner = QueryRunner()
        self.exporter = QueryExporter()
        self.original_data = []
        self.modified_data = []
        self.visible_data = []

        # variables needed for pagination
        self.rows_per_page = 50
        self.rows_loaded = 50

    #   Connect Signals
        self.query_runner.signals.results.connect(self.process_results)
        self.query_results_signal.connect(self.update_view)

    def add_row(self, ix: QtCore.QModelIndex) -> None:
        dummies = {
            FieldType.Bool: True,
            FieldType.Int: None,
            FieldType.Float: 0.00,
            FieldType.Str: '',
            FieldType.Date: None
        }
        dummy_row = []  # type: List
        for fld in self.config.display_base.fields:
            if fld.default_value:
                dummy_row.append(fld.default_value)
            else:
                dummy_row.append(dummies[fld.dtype])
        for k, v in self.config.foreign_keys_by_original_index.items(): # self.foreign_keys.items():
            dummy_row[k] = None

        dummy_row[self.config.primary_key_index] = uuid.uuid4().int
        self.beginInsertRows(QModelIndex(), 0, 0)
        self.visible_data.insert(ix.row() + 1, dummy_row)
        self.modified_data.append(dummy_row)
        self.endInsertRows()

    def canFetchMore(self, index=QtCore.QModelIndex()):
        if len(self.visible_data) > self.rows_loaded:
            return True
        return False

    @property
    def changes(self) -> Optional[Dict[str, set]]:
        if not self.config.table.editable:
            return
        pk = self.config.primary_key_index
        original = set(map(tuple, self.original_data))
        modified = set(map(tuple, self.modified_data))
        changed_ids = set(row[pk] for row in original ^ modified)
        updated = set(
            row for row in modified
            if row[pk] in changed_ids
            and row[pk] in {row[pk] for row in original}
        )
        added = (modified - original) - updated
        deleted = set(
            row for row in (original - modified)
            if row[pk] not in {row[pk] for row in updated}
        )
        return {
            'added': added,
            'deleted': deleted,
            'updated': updated
        }

    def fetchMore(self, index=QtCore.QModelIndex()):
        remainder = len(self.visible_data) - self.rows_loaded
        rows_to_fetch = min(remainder, self.rows_per_page)
        self.beginInsertRows(
            QtCore.QModelIndex(),
            self.rows_loaded,
            self.rows_loaded + rows_to_fetch - 1
        )
        self.rows_loaded += rows_to_fetch
        self.endInsertRows()

    def field_totals(self, col_ix: ColumnIndex) -> list:
        try:
            totals = []
            fld = self.config.fields_by_original_index[col_ix]
            rows = self.rowCount()
            if fld.dtype == FieldType.Float:
                total = sum(val[col_ix] for val in self.visible_data if val[col_ix])
                avg = total / rows if rows > 0 else 0
                totals.append('{} Sum \t = {:,.2f}'.format(fld.name, float(total)))
                totals.append('{} Avg \t = {:,.2f}'.format(fld.name, float(avg)))
            elif fld.dtype == FieldType.Date:
                minimum = min(val[col_ix] for val in self.visible_data
                              if val[col_ix])
                maximum = max(val[col_ix] for val in self.visible_data
                              if val[col_ix])
                totals.append('{} Min \t = {}'.format(fld.name, minimum or 'Empty'))
                totals.append('{} Max \t = {}'.format(fld.name, maximum or 'Empty'))
            else:
                totals.append('{} Distinct Count \t = {}'.format(fld.name
                    , len(set(val[col_ix] for val in self.visible_data if val[col_ix]))))
            return totals
        except Exception as e:
            self.logger.error('field_totals: Error calculating field_totals for '
                              'column {}; error: {}'.format(col_ix, e))

    def columnCount(self, parent: QtCore.QModelIndex=None) -> int:
        return len(self.config.fields)

    def data(self, index: QtCore.QModelIndex, role: int=QtCore.Qt.DisplayRole):
        alignment = {
            FieldType.Bool: QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter,
            FieldType.Date: QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter,
            FieldType.Int: QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter,
            FieldType.Float: QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter,
            FieldType.Str: QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter,
        }
        col = index.column()
        fld = self.config.fields_by_original_index[col]
        val = self.visible_data[index.row()][col]
        try:
            if not index.isValid():
                return
            elif role == QtCore.Qt.TextAlignmentRole:
                if hasattr(fld, 'dimension'):
                    return QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter
                return alignment[fld.dtype]
            elif role == QtCore.Qt.DisplayRole:
                if val is None:
                    return None
                try:
                    if col in self.config.lookup_field_indices:
                        rid = self.visible_data[index.row()][self.config.primary_key_index]
                        return fld.lookup_foreign_key(row_id=rid)
                    elif col in self.config.foreign_key_indices:
                        return fld.lookup_foreign_key(val)
                        # return self.config.foreign_key_lookup(val, col)
                    else:
                        return format_value(
                            field_type=fld.dtype,
                            value=val,
                            field_format=fld.field_format
                        )
                except Exception as e:
                    self.logger.debug('data: error displaying data {}'.format(e))
                    return val
        except Exception as e:
            self.logger.debug(
                'data: error in data method of model: {}'
                .format(e)
            )
            self.error_signal.emit('Error modeling data: {}'.format(str(e)))

    def delete_row(self, ix: QtCore.QModelIndex) -> None:
        row = ix.row()
        pk = self.visible_data[row][self.config.primary_key_index]
        mod_row = next(
            i for i, r
            in enumerate(self.modified_data)
            if r[self.config.primary_key_index] == pk
        )
        del self.visible_data[row]
        del self.modified_data[mod_row]
        self.dataChanged.emit(ix, ix)

    @property
    def displayed_data(self):
        while len(self.visible_data) > self.rows_loaded:
            self.fetchMore()

        rows = []
        for irow in range(self.rowCount()):
            row = []
            for icol in range(self.columnCount()):
                if icol != self.config.primary_key_index:
                    cell = self.data(self.createIndex(irow, icol))
                    row.append(cell)
            rows.append(row)
        return rows

    def distinct_values(self, col_ix: ColumnIndex) -> List[str]:
        # noinspection PyTypeChecker
        return SortedSet(
            str(self.config.foreign_key_lookup(col=col_ix, val=row[col_ix]))
            for row in self.visible_data
        )

    def filter_equality(self, col_ix: ColumnIndex, val: SqlDataType) -> None:
        self.visible_data = [
            row for row in self.visible_data
            if row[col_ix] == val
        ]
        self.filters_changed_signal.emit()

    def filter_greater_than(self, col_ix, val) -> None:
        lkp = partial(self.config.foreign_key_lookup, col=col_ix)
        self.visible_data = [
            row for row in self.visible_data
            if lkp(row[col_ix]) >= lkp(val)
        ]
        self.sort(col=col_ix, order=QtCore.Qt.AscendingOrder)
        self.filters_changed_signal.emit()

    def filter_less_than(self, col_ix, val) -> None:
        lkp = partial(self.config.foreign_key_lookup, col=col_ix)
        self.visible_data = [
            row for row in self.visible_data
            if lkp(row[col_ix]) <= lkp(val)
        ]
        self.sort(col=col_ix, order=QtCore.Qt.DescendingOrder)
        self.filters_changed_signal.emit()

    def filter_like(self, val: str, col_ix: Optional[ColumnIndex]=None) -> None:
        self.layoutAboutToBeChanged.emit()
        lkp = partial(self.config.foreign_key_lookup, col=col_ix)

        def normalize(value: str) -> str:
            return str(value).lower()

        def is_like(input_val: str, row: Tuple[SqlDataType], col: int) -> bool:
            if col:
                if normalize(input_val) in normalize(lkp(row[col])):
                    return True
            else:
                if normalize(input_val) in ' '.join([
                    normalize(self.config.foreign_key_lookup(val=v, col=c))
                    for c, v in enumerate(row)
                ]):
                    return True
            return False

        self.visible_data = [
            row for row in self.modified_data
            if is_like(val, row, col_ix)
        ]

        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    def filter_set(self, col: int, values: Set[str]) -> None:
        try:
            if col in self.config.foreign_key_indices:
                fld_type = FieldType.Str
            else:
                fld_type = self.config.fields_by_original_index[col].dtype
            converter = lambda v: convert_value(field_type=fld_type, value=v)
            vals = set(map(converter, values))
            self.visible_data = [
                row for row in self.visible_data
                if converter(self.config.foreign_key_lookup(col=col, val=row[col])) in vals
            ]
            self.filters_changed_signal.emit()
        except Exception as e:
            self.logger.error(
                'filter_set: Error applying checkbox filter for col {}; '
                'values {}; error {}:'.format(col, values, e)
            )

    def flags(self, ix: QtCore.QModelIndex) -> int:
        if ix.column() in self.config.editable_field_indices:
            return (
                QtCore.Qt.ItemIsEditable
                | QtCore.Qt.ItemIsEnabled
                | QtCore.Qt.ItemIsSelectable
            )
        return QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable

    def export_visible(self, visible_header: List[FieldDisplayName]) -> None:
        self.exporter.start_export(
            rows=self.visible_rows(visible_header),
            header=visible_header,
            table_name=self.config.display_name
        )

    def full_reset(self) -> None:
        self.layoutAboutToBeChanged.emit()
        self.original_data = []
        self.modified_data = []
        self.visible_data = []
        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    @property
    def header(self):
        return [fld.display_name for fld in self.config.fields]

    def headerData(self, col: ColumnIndex, orientation: int, role: QtCore.Qt.DisplayRole) -> List[str]:
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return self.header[col]

    @property
    def pending_changes(self):
        chg = self.changes
        if chg:
            if chg['added'] or chg['deleted'] or chg['updated']:
                return True
            return False
        return False

    def post_processing(self, rows: List[List[SqlDataType]]) -> List[List[SqlDataType]]:
        # TODO
        # insert many-to-many fields
        mod_rows = rows
        if self.config.lookup_fields:
            for row in rows:
                for k, v in self.config.lookup_values_by_original_index.items():
                    if v():
                        row.append(v().get(row[0], ''))
                    else:
                        self.logger.debug(
                            'post_processing: Could not find foreign keys for '
                            'field: {}; row: {}'.format(k, row)
                        )
                        row.append('')

        # update row-level calculated fields
        return mod_rows

    def pull(self, show_rows_returned=True) -> None:
        self.rows_loaded = self.rows_per_page
        try:
            self.query_runner.run_sql(
                query=self.config.display_base.select,
                show_rows_returned=show_rows_returned,
                con_str=self.config.app.db_path
            )
        except AttributeError as e:
            err_msg = 'pull: {} does not a valid sql statement associated ' \
                      'with it; error: {}'\
                      .format(self.config.table.display_name, e)
            self.logger.error(err_msg)
            raise
        except Exception as e:
            err_msg = "Error compising the query for table {}; error: {}" \
                      .format(self.base.display_name, e)
            self.logger.error('pull: {}'.format(err_msg))
            raise

    @QtCore.pyqtSlot(list)
    def process_results(self, results: list) -> None:
        """Convert data to specified data types"""
        processed = []
        try:
            for r, row in enumerate(results):
                processed.append(list(row))
                for c, col in enumerate(row):
                    try:
                        field_type = self.config.fields_by_original_index[c].dtype
                        processed[r][c] = convert_value(
                            field_type=field_type,
                            value=col
                        )
                    except Exception as e:
                        self.logger.debug(
                            'process_results: Error converting value {}, '
                            'row {}, col {}, err {}'.format(col, r, c, e)
                        )
                        processed[r][c] = col
            self.query_results_signal.emit(processed)
        except Exception as e:
            err_msg = "Error processing results: {}".format(e)
            self.logger.debug('process_results: {}'.format(err_msg))
            self.error_signal.emit(err_msg)

    def primary_key(self, row: int) -> int:
        """Return the primary key value of the specified row"""
        return row[self.config.primary_key_index]

    @QtCore.pyqtSlot(str)
    def query_errored(self, msg) -> None:
        self.error_signal.emit(msg)

    def reset(self) -> None:
        """reset filters - not pending changes"""
        self.layoutAboutToBeChanged.emit()
        self.visible_data = self.modified_data
        self.filters_changed_signal.emit()
        self.layoutChanged.emit()

    def rowCount(self, index: Optional[QtCore.QModelIndex]=None) -> int:
        if self.visible_data:
            if len(self.visible_data) <= self.rows_loaded:
                return len(self.visible_data)
            return self.rows_loaded
        return 0

    def save(self) -> Optional[Dict[str, int]]:
        if self.pending_changes:
            try:
                results = self.config.table.save_changes(self.changes)
                def update_id(old_id, new_id):
                    row = next(
                        i for i, row in enumerate(self.modified_data)
                        if row[self.config.primary_key_index] == old_id
                    )
                    self.modified_data[row][self.config.primary_key_index] = new_id

                for m in results['new_rows_id_map']:
                    update_id(m[0], m[1])

                self.original_data = deepcopy(self.modified_data)
                # if self.config.display_base.refresh_on_update:
                #     self.pull(show_rows_returned=False)
                return results
            except Exception as e:
                self.logger.error(
                    'save: There was an error saving the following changes: {};'
                    'error: {}'.format(self.changes, e)
                )
                raise
        # else no changes to save, view displays 'no changes' when this function returns None

    def setData(self, ix: QtCore.QModelIndex, value: SqlDataType, role: int=QtCore.Qt.EditRole) -> bool:
        try:
            value = convert_value(
                field_type=self.config.fields_by_original_index[ix.column()].dtype,
                value=value
            )
        except Exception as e:
            self.error_signal.emit(str(e))
            return False
        try:
            pk = self.visible_data[ix.row()][self.config.primary_key_index]
            row = next(
                i for i, row
                in enumerate(self.modified_data)
                if row[self.config.primary_key_index] == pk
            )
            self.visible_data[ix.row()][ix.column()] = value
            self.modified_data[row][ix.column()] = value
            self.dataChanged.emit(ix, ix)
            return True
        except Exception as e:
            self.error_signal.emit(str(e))
            return False

    def sort(self, col: ColumnIndex, order: int) -> None:
        """sort table by given column number col"""
        try:
            self.layoutAboutToBeChanged.emit()
            if col in self.config.foreign_key_indices:
                self.visible_data = sorted(
                    self.visible_data
                    , key=lambda row: self.config.foreign_key_lookup(row[col], col)
                )
            else:
                try:
                    self.visible_data = sorted(
                        self.visible_data
                        , key=lambda row: row[col] or 0
                    )
                except Exception as e:
                    self.logger.error('sort: Could not sort rows by their native '
                                      'data type; err: {}; reverting to sorting '
                                      'by string value'.format(e))
                    self.visible_data = sorted(
                        self.visible_data
                        , key=lambda row: str(row[col] or 0).lower()
                    )
            if order == QtCore.Qt.DescendingOrder:
                self.visible_data.reverse()
            self.layoutChanged.emit()
        except Exception as e:
            err_msg = "Error sorting data; col: {} err: {}".format(col, e)
            self.logger.error('sort: {}'.format(err_msg))
            self.error_signal.emit(err_msg)

    def undo(self) -> None:
        if self.pending_changes:
            self.layoutAboutToBeChanged.emit()
            self.modified_data = deepcopy(self.original_data)
            self.visible_data = deepcopy(self.original_data)
            self.layoutChanged.emit()
        else:
            self.error_signal.emit('No changes to undo')

    @QtCore.pyqtSlot(list)
    def update_view(self, results) -> None:
        self.layoutAboutToBeChanged.emit()
        # self.original_data = results
        self.original_data = self.post_processing(results)
        self.visible_data = deepcopy(self.original_data)
        self.modified_data = deepcopy(self.original_data)
        self.layoutChanged.emit()

    def visible_rows(self, visible_header: List[FieldDisplayName]) \
            -> List[List[str]]:
        """The data as it is displayed to the user

        This property is used as the data source for exporting the table"""
        pk = self.config.primary_key_index

        def convert_row(row):
            return [
                self.config.foreign_key_lookup(val=val, col=col)
                for col, val
                in enumerate(row)
                if col != pk
            ]

        if self.visible_data:
            original_display_header = [
                fld.display_name
                for fld in self.config.fields
                if fld.visible
            ]
            visible_header_map = {
                hdr: i
                for i, hdr in enumerate(visible_header)
            }
            col_map = {
                visible_header_map[hdr]: i
                for i, hdr in enumerate(original_display_header)
            }
            original_rows = [
                convert_row(row)
                for row in self.visible_data
            ]
            map_row = lambda row: [row[col_map[ix]] for ix, _ in enumerate(row)]
            return [map_row(row) for row in original_rows]

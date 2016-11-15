from copy import deepcopy
from functools import partial
import uuid

from PyQt4.QtCore import QModelIndex
from typing import (
    Dict,
    List,
    Optional, Tuple, Set
)

from PyQt4 import QtCore
from sortedcontainers import SortedSet

from logger import rotating_log
from star_schema.config import cfg
from star_schema.custom_types import ColumnIndex, SqlDataType, FieldIndex
from query_manager import QueryManager

from star_schema.custom_types import FieldType
from star_schema.constellation import (
    Table,
    ForeignKey,
    Field
)
from star_schema.constellation import format_value, convert_value


class AbstractModel(QtCore.QAbstractTableModel):
    filters_changed_signal = QtCore.pyqtSignal()
    error_signal = QtCore.pyqtSignal(str)

    def __init__(self, table: Table):
        super(AbstractModel, self).__init__()
        self.query_manager = QueryManager(table=table)
        self.original_data = []
        self.modified_data = []
        self.visible_data = []

        self.logger = rotating_log('model.AbstractModel')

        # variables needed for pagination
        self.rows_per_page = 50
        self.rows_loaded = 50

    #   Connect Signals
        self.query_manager.query_results_signal.connect(self.update_view)

    def add_row(self, ix: QtCore.QModelIndex) -> None:
        dummies = {
            FieldType.Bool: True
            , FieldType.Int: None  #0
            , FieldType.Float: 0.00
            , FieldType.Str: ''
            , FieldType.Date: None #'1900-01-01'
        }
        dummy_row = []  # type: List
        for fld in self.query_manager.base.fields:
            if fld.default_value:
                dummy_row.append(fld.default_value)
            else:
                dummy_row.append(dummies[fld.dtype])
        for k, v in self.query_manager.table.foreign_keys.items():
            dummy_row[k] = None #next(fk for fk in self.foreign_keys[k])

        dummy_row[self.query_manager.table.primary_key_index] = uuid.uuid4().int
        self.beginInsertRows(QModelIndex(), 0, 0)
        self.visible_data.insert(ix.row() + 1, dummy_row)
        self.modified_data.append(dummy_row)
        self.endInsertRows()
        # self.dataChanged.emit(ix, ix)

    def canFetchMore(self, index=QtCore.QModelIndex()):
        if len(self.visible_data) > self.rows_loaded:
            return True
        return False

    @property
    def changes(self) -> Dict[str, set]:
        if not self.query_manager.table.editable:
            return  # safe guard
        pk = self.query_manager.table.primary_key_index
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
            'added': added
            , 'deleted': deleted
            , 'updated': updated
        }

    def fetchMore(self, index=QtCore.QModelIndex()):
        remainder = len(self.visible_data) - self.rows_loaded
        rows_to_fetch = min(remainder, self.rows_per_page)
        self.beginInsertRows(
            QtCore.QModelIndex()
            , self.rows_loaded
            , self.rows_loaded + rows_to_fetch - 1
        )
        self.rows_loaded += rows_to_fetch
        self.endInsertRows()

    def field_totals(self, col_ix: ColumnIndex) -> list:
        try:
            totals = []
            fld = self.query_manager.fields[col_ix]
            rows = self.rowCount()
            if fld.dtype == FieldType.Float:
                total = sum(val[col_ix] for val in self.visible_data if val[col_ix])
                avg = total / rows if rows > 0 else 0
                totals.append('{} Sum \t = {:,.2f}'.format(fld.name, float(total)))
                totals.append('{} Avg \t = {:,.2f}'.format(fld.name, float(avg)))
            elif fld.dtype == FieldType.Date:
                minimum = min(val[col_ix] for val in self.visible_data if val[col_ix])
                maximum = max(val[col_ix] for val in self.visible_data if val[col_ix])
                totals.append('{} Min \t = {}'.format(fld.name, minimum or 'Empty'))
                totals.append('{} Max \t = {}'.format(fld.name, maximum or 'Empty'))
            else:
                totals.append('{} Distinct Count \t = {}'.format(fld.name
                    , len(set(val[col_ix] for val in self.visible_data if val[col_ix]))))
            return totals
        except Exception as e:
            self.logger.error('field_totals: Error calculating field_totals for '
                              'column {}; error: {}'.format(col_ix, str(e)))

    def columnCount(self, parent: QtCore.QModelIndex=None) -> int:
        return len(self.query_manager.fields)

    def data(self, index: QtCore.QModelIndex, role: int=QtCore.Qt.DisplayRole):
        alignment = {
            FieldType.Bool: QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter,
            FieldType.Date: QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter,
            FieldType.Int: QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter,
            FieldType.Float: QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter,
            FieldType.Str: QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter,
        }
        col = index.column()
        fld = self.query_manager.fields[col]
        val = self.visible_data[index.row()][col]
        try:
            if not index.isValid():
                return
            elif role == QtCore.Qt.TextAlignmentRole:
                if isinstance(fld, ForeignKey):
                    return QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter
                return alignment[fld.dtype]
            elif role == QtCore.Qt.DisplayRole:
                if val is None:
                    return None
                try:
                    if col in self.foreign_keys.keys():
                        return self.foreign_keys[col][val]
                    return format_value(
                        field_type=fld.dtype,
                        value=val,
                        field_format=fld.field_format
                    )
                except Exception as e:
                    self.logger.debug('data: error displaying data {}'.format(str(e)))
                    return val
        except Exception as e:
            self.logger.debug('data: error in data method of model: {}'
                              .format(str(e)))
            self.error_signal.emit('Error modeling data: {}'.format(e))

    def delete_row(self, ix: QtCore.QModelIndex) -> None:
        row = ix.row()
        pk = self.visible_data[row][self.query_manager.table.primary_key_index]
        mod_row = next(
            i for i, r
            in enumerate(self.modified_data)
            if r[self.query_manager.table.primary_key_index] == pk
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
                if icol != self.query_manager.table.primary_key_index:
                    cell = self.data(self.createIndex(irow, icol))
                    row.append(cell)
            rows.append(row)
        return rows

    def distinct_values(self, col_ix: ColumnIndex) -> List[str]:
        return SortedSet(
            str(self.fk_lookup(col=col_ix, val=row[col_ix]))
            for row in self.visible_data
        )

    def field_by_id(self, ix: FieldIndex) -> Field:
        return self.query_manager.fields[ix]

    def filter_equality(self, col_ix: ColumnIndex, val: SqlDataType) -> None:
        self.visible_data = [
            row for row in self.visible_data
            if row[col_ix] == val
        ]
        self.filters_changed_signal.emit()

    def filter_greater_than(self, col_ix, val) -> None:
        lkp = partial(self.fk_lookup, col=col_ix)
        self.visible_data = [
            row for row in self.visible_data
            if lkp(row[col_ix]) >= lkp(val)
        ]
        self.sort(col=col_ix, order=QtCore.Qt.AscendingOrder)
        self.filters_changed_signal.emit()

    def filter_less_than(self, col_ix, val) -> None:
        lkp = partial(self.fk_lookup, col=col_ix)
        self.visible_data = [
            row for row in self.visible_data
            if lkp(row[col_ix]) <= lkp(val)
        ]
        self.sort(col=col_ix, order=QtCore.Qt.DescendingOrder)
        self.filters_changed_signal.emit()

    def filter_like(self, val: str, col_ix: Optional[ColumnIndex]=None) -> None:
        self.layoutAboutToBeChanged.emit()
        lkp = partial(self.fk_lookup, col=col_ix)

        def normalize(value: str) -> str:
            return str(value).lower()

        def is_like(input_val: str, row: Tuple[SqlDataType], col: int) -> bool:
            if col:
                if normalize(input_val) in normalize(lkp(row[col])):
                    return True
            else:
                if normalize(input_val) in ' '.join([
                    normalize(self.fk_lookup(val=v, col=c))
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
            fld_type = FieldType.Str if col in self.foreign_keys \
                       else self.field_by_id(col).dtype
            converter = lambda v: convert_value(field_type=fld_type, value=v)
            vals = set(map(converter, values))
            self.visible_data = [
                row for row in self.visible_data
                if converter(self.fk_lookup(col=col, val=row[col])) in vals
            ]
            self.filters_changed_signal.emit()
        except Exception as e:
            self.logger.error('filter_set: Error applying checkbox filter for col {}; '
                              'values {}; error {}:'.format(col, values, str(e)))

    def flags(self, ix: QtCore.QModelIndex) -> int:
        if ix.column() in self.query_manager.editable_fields_indices:
            return (
                QtCore.Qt.ItemIsEditable
                | QtCore.Qt.ItemIsEnabled
                | QtCore.Qt.ItemIsSelectable
            )
        return QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable

    def fk_lookup(self, val, col) -> SqlDataType:
        if col in self.query_manager.table.foreign_keys.keys():
            try:
                return self.foreign_keys[col][val]
            except KeyError:
                return ''
        return val

    @property
    def foreign_keys(self) -> Dict[ColumnIndex, Dict[int, str]]:
        try:
            return {
                ColumnIndex(k): cfg.foreign_keys(v.dimension)
                for k, v
                in self.query_manager.table.foreign_keys.items()
            }
        except Exception as e:
            self.logger.error("foreign_keys: "
                              "The model can't find the foreign keys; "
                              "error {}".format(str(e)))

    def full_reset(self) -> None:
        self.layoutAboutToBeChanged.emit()
        self.original_data = []
        self.modified_data = []
        self.visible_data = []
        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    def headerData(self, col: ColumnIndex, orientation: int, role: QtCore.Qt.DisplayRole) -> List[str]:
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return self.query_manager.headers[col]

    @property
    def pending_changes(self):
        chg = self.changes
        if chg:
            if chg['added'] or chg['deleted'] or chg['updated']:
                return True
            return False
        return False

    def pull(self, show_rows_returned=True) -> None:
        self.rows_loaded = self.rows_per_page
        self.query_manager.pull(show_rows_returned)

    def primary_key(self, row: int) -> int:
        """Return the primary key value of the specified row"""
        return row[self.query_manager.table.primary_key_index]

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
                # print('changes:', self.changes)
                results = self.query_manager.save_changes(self.changes)

                def update_id(old_id, new_id):
                    row = next(
                        i for i, row in enumerate(self.modified_data)
                        if row[self.query_manager.table.primary_key_index] == old_id
                    )
                    self.modified_data[row][self.query_manager.table.primary_key_index] = new_id

                for m in results['new_rows_id_map']:
                    update_id(m[0], m[1])

                self.original_data = deepcopy(self.modified_data)

                if self.query_manager.table in cfg.dimensions:
                    cfg.pull_foreign_keys(self.query_manager.table.table_name)
                if self.query_manager.base.refresh_on_update:
                    self.pull(show_rows_returned=False)
                return results
            except:
                raise
        # else no changes to save, view displays 'no changes' when this function returns None

    def setData(self, ix: QtCore.QModelIndex, value: SqlDataType, role: int=QtCore.Qt.EditRole) -> bool:
        try:
            value = convert_value(
                field_type=self.query_manager.fields[ix.column()].dtype,
                value=value
            )
        except:
            pass
        try:
            pk = self.visible_data[ix.row()][self.query_manager.table.primary_key_index]
            row = next(
                i for i, row
                in enumerate(self.modified_data)
                if row[self.query_manager.table.primary_key_index] == pk
            )
            self.visible_data[ix.row()][ix.column()] = value
            self.modified_data[row][ix.column()] = value
            self.dataChanged.emit(ix, ix)
            return True
        except:
            return False

    def sort(self, col: ColumnIndex, order: int) -> None:
        """sort table by given column number col"""
        try:
            self.layoutAboutToBeChanged.emit()
            if col in self.foreign_keys.keys():
                self.visible_data = sorted(
                    self.visible_data
                    , key=lambda row: self.fk_lookup(row[col], col)
                )
            else:
                try:
                    self.visible_data = sorted(
                        self.visible_data
                        , key=lambda row: row[col] or 0 #operator.itemgetter(col)
                    )
                except Exception as e:
                    self.logger.error('sort: Could not sort rows by their native '
                                      'data type; err: {}; reverting to sorting '
                                      'by string value'.format(str(e)))
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
        self.original_data = results
        self.visible_data = deepcopy(results)
        self.modified_data = deepcopy(results)
        self.layoutChanged.emit()

    @property
    def visible_rows(self):
        """The data as it is displayed to the user

        This property is used as the data source for exporting the table"""
        pk = self.query_manager.table.primary_key_index

        def convert_row(row):
            return (
                self.fk_lookup(val=val, col=col)
                for col, val
                in enumerate(row)
                if col != pk
            )

        if self.visible_data:
            return [
                convert_row(row)
                for row in self.visible_data
            ]

    @property
    def visible_header(self):
        pk = self.query_manager.table.primary_key_index
        return [
            hdr
            for col, hdr
            in enumerate(self.query_manager.headers)
            if col != pk
        ]

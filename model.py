from copy import deepcopy
import operator
from functools import partial
import uuid

from sortedcollections import ValueSortedDict
from typing import Any, Dict

from PyQt4 import QtCore, QtGui
from sortedcontainers import SortedSet

from config import cfg
from db import fetch
from logger import log_error
from query_manager import QueryManager
from schema import FieldType, Table
from utilities import immutable_property


class AbstractModel(QtCore.QAbstractTableModel):
    filters_changed_signal = QtCore.pyqtSignal()
    error_signal = QtCore.pyqtSignal(str)

    def __init__(self, table: Table):
        super(AbstractModel, self).__init__()
        self.query_manager = QueryManager(table=table)
        self.original_data = []
        self.modified_data = []
        self.visible_data = []

        # variables needed for pagination
        self.rows_per_page = 50
        self.rows_loaded = 50

    #   Connect Signals
        self.query_manager.query_results_signal.connect(self.update_view)

    def add_row(self, ix: QtCore.QModelIndex) -> None:
        dummies = {
            FieldType.int: 0
            , FieldType.float: 0.0
            , FieldType.str: ''
            , FieldType.date: '1900-01-01'
        }
        dummy_row = []  # type: list
        for fld in self.query_manager.table.fields:
            dummy_row.append(dummies[fld.dtype])
        for i in self.foreign_keys.keys():
            dummy_row[i] = next(fk for fk in self.foreign_keys[i])
        dummy_row[self.query_manager.table.primary_key_index] = uuid.uuid4().int
        self.visible_data.insert(ix.row(), dummy_row)
        self.modified_data.insert(0, dummy_row)
        self.dataChanged.emit(ix, ix)

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

    def field_totals(self, col_ix: int) -> list:
        totals = []
        fld = self.query_manager.table.fields[col_ix]
        if fld.dtype == FieldType.float:
            total = sum(val[col_ix] for val in self.visible_data)
            avg = total / self.rowCount() if self.rowCount() > 0 else 0
            totals.append('{} Sum \t = {:,.2f}'.format(fld.name, float(total)))
            totals.append('{} Avg \t = {:,.2f}'.format(fld.name, float(avg)))
        elif fld.dtype == FieldType.date:
            minimum = min(val[col_ix] for val in self.visible_data)
            maximum = max(val[col_ix] for val in self.visible_data)
            totals.append('{} Min \t = {}'.format(fld.name, minimum or 'Empty'))
            totals.append('{} Max \t = {}'.format(fld.name, maximum or 'Empty'))
        else:
            totals.append('{} Distinct Count \t = {}'.format(fld.name
                , len(set(val[col_ix] for val in self.visible_data))))
        return totals

    @immutable_property
    def foreign_keys(self) -> Dict[int, Dict[int, str]]:
        fks = {}
        for col, fld in self.query_manager.table.foreign_keys.items():
            fks[col] = ValueSortedDict({
                row[0]: row[1]
                for row in fetch(cfg.foreign_key_lookups[fld.dimension])
            })
        return fks

    def columnCount(self, parent: QtCore.QModelIndex=None) -> int:
        return len(self.query_manager.table.fields)

    @log_error
    def data(self, index: QtCore.QModelIndex, role: int=QtCore.Qt.DisplayRole):
        alignment = {
            FieldType.date: QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter,
            FieldType.int: QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter,
            FieldType.float: QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter,
            FieldType.str: QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter,
        }
        fld = self.query_manager.table.fields[index.column()]
        val = self.visible_data[index.row()][index.column()]
        try:
            if not index.isValid():
                return
            elif role == QtCore.Qt.TextAlignmentRole:
                return alignment[fld.dtype]
            elif role == QtCore.Qt.DisplayRole:
                if index.column() in self.foreign_keys.keys():
                    return self.foreign_keys[index.column()][val]
                return fld.format_value(val)
        except Exception as e:
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

    def distinct_values(self, col_ix):
        return SortedSet(
            str(self.fk_lookup(col=col_ix, val=row[col_ix]))
            for row in self.visible_data
        )

    def filter_equality(self, col_ix, val):
        self.visible_data = [
            row for row in self.visible_data
            if row[col_ix] == val
        ]
        self.filters_changed_signal.emit()

    def filter_greater_than(self, col_ix, val):
        lkp = partial(self.fk_lookup, col=col_ix)
        self.visible_data = [
            row for row in self.visible_data
            if lkp(row[col_ix]) >= lkp(val)
        ]
        # self.sort(col=col_ix, order=QtCore.Qt.AscendingOrder)
        self.filters_changed_signal.emit()

    def filter_less_than(self, col_ix, val):
        lkp = partial(self.fk_lookup, col=col_ix)
        self.visible_data = [
            row for row in self.visible_data
            if lkp(row[col_ix]) <= lkp(val)
        ]
        # self.sort(col=col_ix, order=QtCore.Qt.DescendingOrder)
        self.filters_changed_signal.emit()

    def filter_like(self, val, col_ix=None):
        self.layoutAboutToBeChanged.emit()
        lkp = partial(self.fk_lookup, col=col_ix)

        def normalize(val):
            return str(val).lower()

        def is_like(input_val, row, col):
            if col:
                if normalize(input_val) in normalize(lkp(row[col])):
                    return True
            else:
                if normalize(input_val) in ' '.join([
                    normalize(self.fk_lookup(val=v, col=c))
                    for c, v in enumerate(row)
                ]):
                    return True

        self.visible_data = [
            row for row in self.visible_data
            if is_like(val, row, col_ix)
        ]

        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    def filter_set(self, col_ix, values):
        self.visible_data = [
            row for row in self.visible_data
            if str(self.fk_lookup(col=col_ix, val=row[col_ix]))
               in [y for y in values]
        ]
        self.filters_changed_signal.emit()

    def flags(self, ix: QtCore.QModelIndex) -> int:
        if ix.column() in self.query_manager.editable_fields_indices:
            return (
                QtCore.Qt.ItemIsEditable
                | QtCore.Qt.ItemIsEnabled
                | QtCore.Qt.ItemIsSelectable
            )
        return QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable

    def fk_lookup(self, val, col):
        if col in self.foreign_keys.keys():
            return self.foreign_keys.get(col).get(val)
        return val

    def full_reset(self):
        self.layoutAboutToBeChanged.emit()
        self.original_data = []
        self.modified_data = []
        self.visible_data = []
        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    def headerData(self, col, orientation, role):
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return self.query_manager.headers[col]

    def pull(self):
        self.rows_loaded = self.rows_per_page
        self.query_manager.pull()

    def primary_key(self, row: int) -> int:
        """Return the primary key value of the specified row"""
        return row[self.query_manager.table.primary_key_index]

    @QtCore.pyqtSlot(str)
    def query_errored(self, msg):
        self.error_signal.emit(msg)

    def reset(self):
        """reset filters - not pending changes"""
        self.layoutAboutToBeChanged.emit()
        self.visible_data = self.modified_data
        self.filters_changed_signal.emit()
        self.layoutChanged.emit()

    def rowCount(self, index: QtCore.QModelIndex):
        if self.visible_data:
            if len(self.visible_data) <= self.rows_loaded:
                return len(self.visible_data)
            return self.rows_loaded
        return 0

    def save(self) -> Dict[str, int]:
        chg = self.changes
        if chg['added'] or chg['deleted'] or chg['updated']:
            try:
                results = self.query_manager.save_changes(chg)

                def update_id(old_id, new_id):
                    row = next(
                        i for i, row in enumerate(self.modified_data)
                        if row[self.query_manager.table.primary_key_index] == old_id
                    )
                    self.modified_data[row][self.query_manager.table.primary_key_index] = new_id

                for m in results['new_rows_id_map']:
                    update_id(m[0], m[1])

                self.original_data = deepcopy(self.modified_data)

                return results
            except:
                raise
        # else no changes to save, view displays 'no changes' when this function returns None

    def setData(self, ix: QtCore.QModelIndex, value: Any, role: int=QtCore.Qt.EditRole) -> bool:
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

    def sort(self, col, order):
        """sort table by given column number col"""
        try:
            self.layoutAboutToBeChanged.emit()
            if col in self.foreign_keys.keys():
                self.visible_data = sorted(
                    self.visible_data
                    , key=lambda row: self.foreign_keys[col][row[col]]
                )
            else:
                self.visible_data = sorted(
                    self.visible_data
                    , key=operator.itemgetter(col)
                )
            if order == QtCore.Qt.DescendingOrder:
                self.visible_data.reverse()
            self.layoutChanged.emit()
        except Exception as e:
            err_msg = "Error sorting data: {}".format(e)
            self.error_signal.emit(err_msg)

    def undo(self) -> None:
        self.layoutAboutToBeChanged.emit()
        self.modified_data = deepcopy(self.original_data)
        self.visible_data = deepcopy(self.original_data)
        self.layoutChanged.emit()

    @QtCore.pyqtSlot(list)
    def update_view(self, results):
        self.layoutAboutToBeChanged.emit()
        self.original_data = results
        self.visible_data = deepcopy(results)
        self.modified_data = deepcopy(results)
        self.layoutChanged.emit()
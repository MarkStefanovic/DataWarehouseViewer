from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import operator
import re
import threading
from typing import Any, Dict
import uuid

from PyQt4 import QtCore
from sortedcontainers import SortedSet

from logger import log_error
from query_manager import QueryManager
from utilities import immutable_property, is_float


class AbstractModel(QtCore.QAbstractTableModel):
    filters_changed_signal = QtCore.pyqtSignal()
    error_signal = QtCore.pyqtSignal(str)
    exit_signal = QtCore.pyqtSignal()
    rows_returned_signal = QtCore.pyqtSignal(str)
    rows_exported_signal = QtCore.pyqtSignal(int)

    def __init__(self, config):
        super(AbstractModel, self).__init__()
        self._query_manager = QueryManager(config)
        self._original_data = []
        self._modified_data = []

        # variables needed for pagination
        self.rows_per_page = 50
        self.rows_loaded = 50

    #   Connect Signals
        self.exit_signal.connect(self._query_manager.exit_signal.emit)
        self._query_manager.error_signal.connect(self.error_signal.emit)
        self._query_manager.query_results_signal.connect(self.update_view)
        self._query_manager.rows_exported_signal.connect(self.rows_exported_signal.emit)
        self._query_manager.rows_returned_signal.connect(self.rows_returned_signal.emit)

    def canFetchMore(self, index=QtCore.QModelIndex()):
        if len(self._modified_data) > self.rows_loaded:
            return True
        return False

    @property
    def changes(self) -> Dict[str, set]:
        pk = self._query_manager.primary_key_index
        original = SortedSet(map(tuple, self._original_data))
        modified = SortedSet(map(tuple, self._modified_data))
        changed_ids = SortedSet(row[pk] for row in original ^ modified)
        updated = SortedSet(
            row
            for row in modified
            if row[pk] in changed_ids
            and row[pk] in {
                row[pk]
                for row
                in original
            }
        )
        added = (modified - original) - updated
        deleted = SortedSet(
            row
            for row in (original - modified)
            if row[pk] not in {
                row[pk]
                for row
                in updated
            }
        )
        return {
            'added': added
            , 'deleted': deleted
            , 'updated': updated
        }

    @immutable_property
    def editable(self):
        return self._query_manager.editable

    def export(self):
        self._query_manager.export()

    def fetchMore(self, index=QtCore.QModelIndex()):
        remainder = len(self._modified_data) - self.rows_loaded
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
        fld = self._query_manager.fields.get(col_ix)
        if fld.type == 'float':
            total = sum(val[col_ix] for val in self._modified_data if is_float(val[col_ix]))
            avg = total / self.rowCount() if self.rowCount() > 0 else 0
            totals.append('{} Sum \t = {:,.2f}'.format(fld.name, float(total)))
            totals.append('{} Avg \t = {:,.2f}'.format(fld.name, float(avg)))
        elif fld.type == 'date':
            minimum = min(val[col_ix] for val in self._modified_data)
            maximum = max(val[col_ix] for val in self._modified_data)
            totals.append('{} Min \t = {}'.format(fld.name, minimum or 'Empty'))
            totals.append('{} Max \t = {}'.format(fld.name, maximum or 'Empty'))
        else:
            totals.append('{} Distinct Count \t = {}'.format(fld.name
                , len(set(val[col_ix] for val in self._modified_data))))
        return totals

    @immutable_property
    def foreign_keys(self):
        return self._query_manager.foreign_keys

    def columnCount(self, parent=None):
        return len(self._modified_data[0]) if self._modified_data else 0

    @log_error
    def data(self, index, role):
        # TODO: allow user to specify format in config file
        def formatter(value, fmt):
            format_options = {
                'currency': '${:,.2f}'
                , 'date': None
                , 'datetime': None
                , 'dollar': '${:,.0f}'
                , 'standard': '{:,.2f}'
                , 'str': None
            }
            if format_options.get(fmt):
                format_str = format_options.get(fmt)
                try:
                    return format_str.format(value)
                except:
                    # global_message_queue.errored('Error formatting {v} as {f}'.format(v=val, f=fmt))
                    return value
            return value

        col_type = self._query_manager.fields.get(index.column()).type
        val = self._modified_data[index.row()][index.column()]
        try:
            if not index.isValid():
                return
            elif role == QtCore.Qt.TextAlignmentRole:
                if col_type == 'float':
                    return QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter
                elif col_type == 'date':
                    return QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter
                else:
                    return QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter
            elif role != QtCore.Qt.DisplayRole:
                return
            else:
                if col_type == 'float':
                    # and is_float(val): #re.match("^\d+\.\d+$", val):
                    return formatter(val, 'standard')
                elif col_type == 'date':
                    return val[:10]
                return val
        except Exception as e:
            err_msg = 'Error modeling data: {}'.format(e)
            self.error_signal.emit(err_msg)

    def distinct_values(self, col_ix):
        return SortedSet(str(val[col_ix]) for val in self._modified_data)

    @immutable_property
    def editable_fields(self):
        return self._query_manager.editable_fields

    def filter_equality(self, col_ix, val):
        self._modified_data = [x for x in self._modified_data if x[col_ix] == val]
        self.filters_changed_signal.emit()

    def filter_greater_than(self, col_ix, val):
        self._modified_data = [x for x in self._modified_data if x[col_ix] >= val]
        self.filters_changed_signal.emit()

    def filter_less_than(self, col_ix, val):
        self._modified_data = [x for x in self._original_data if x[col_ix] <= val]
        self.filters_changed_signal.emit()

    def filter_like(self, val, col_ix=None):
        self.layoutAboutToBeChanged.emit()

        def normalize(val):
            return str(val).lower()

        def is_like(input_val, row, col):
            if col:
                if normalize(input_val) in normalize(row[col]):
                    return True
            else:
                if normalize(input_val) in ' '.join([normalize(v) for v in row]):
                    return True

        self._modified_data = [
            line
            for line in self._original_data
            if is_like(val, line, col_ix)
        ]

        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    def filter_set(self, col_ix, values):
        self._modified_data = [
            x for x in self._original_data
            if str(x[col_ix]) in [y for y in values]
        ]
        self.filters_changed_signal.emit()

    def flags(self, ix: QtCore.QModelIndex) -> int:
        if ix.column() in self._query_manager.editable_fields:
            return (
                QtCore.Qt.ItemIsEditable
                | QtCore.Qt.ItemIsEnabled
                | QtCore.Qt.ItemIsSelectable
            )
        return QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable

    # @immutable_property
    # def foreign_keys(self):
    #     return self._query_manager.foreign_keys

    def full_reset(self):
        self.layoutAboutToBeChanged.emit()
        self._original_data = []
        self._modified_data = []
        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    def headerData(self, col, orientation, role):
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return self.header[col]
        return None

    @immutable_property
    def header(self):
        return self._query_manager.headers

    def insertRow(self, row: int, ix: QtCore.QModelIndex) -> bool:
        dummies = {
            'int': 0
            , 'float': 0.0
            , 'str': ''
            , 'date': '1900-01-01'
        }
        dummy_row = []  # type: list
        for _, field in sorted(self._query_manager.fields.items()):
            dummy_row.append(dummies.get(field.type))
        #replace id column with a random uuid
        dummy_row[0] = uuid.uuid4().int

        self._modified_data.insert(
            ix.row()
            , dummy_row
        )
        self.dataChanged.emit(ix, ix)
        return True

    def pull(self):
        self.rows_loaded = self.rows_per_page
        self._query_manager.pull()

    @immutable_property
    def primary_key_index(self):
        return self._query_manager.primary_key_index

    @QtCore.pyqtSlot(str)
    def query_errored(self, msg):
        self.error_signal.emit(msg)

    def removeRow(self, row: int, ix: QtCore.QModelIndex) -> bool:
        del self._modified_data[ix.row()]
        self.dataChanged.emit(ix, ix)
        return True

    def reset(self):
        self.layoutAboutToBeChanged.emit()
        self._modified_data = self._original_data
        self.filters_changed_signal.emit()
        self.layoutChanged.emit()

    def rowCount(self, index=QtCore.QModelIndex()):
        if self._modified_data:
            if len(self._modified_data) <= self.rows_loaded:
                return len(self._modified_data)
            return self.rows_loaded
        else:
            return 0

    def save(self) -> Dict[str, int]:
        chg = self.changes
        if chg['added'] or chg['deleted'] or chg['updated']:
            results = self._query_manager.save_changes(chg)
            if results['rows_errored']:
                self.error_signal.emit("Error saving changes")
            else:
                self._original_data = deepcopy(self._modified_data)
            return results
        else:
            return {}
        # TODO:
        # any records that we are unable to save due to validation errors
        # filter on them so they can be fixed

    def setData(self, ix: QtCore.QModelIndex, value: Any, role: int=QtCore.Qt.EditRole) -> bool:
        self._modified_data[ix.row()][ix.column()] = value
        self.dataChanged.emit(ix, ix)
        return True

    def sort(self, col, order):
        """sort table by given column number col"""
        try:
            self.layoutAboutToBeChanged.emit()
            self._modified_data = sorted(
                self._modified_data
                , key=operator.itemgetter(col)
            )
            if order == QtCore.Qt.DescendingOrder:
                self._modified_data.reverse()
            self.layoutChanged.emit()
        except Exception as e:
            err_msg = "Error sorting data: {}".format(e)
            self.error_signal.emit(err_msg)

    def undo(self) -> None:
        self.layoutAboutToBeChanged.emit()
        self._modified_data = deepcopy(self._original_data)
        self.layoutChanged.emit()

    @QtCore.pyqtSlot(list)
    def update_view(self, results):
        self.layoutAboutToBeChanged.emit()
        self._original_data = results
        self._modified_data = deepcopy(results)
        self.filters_changed_signal.emit()
        self.layoutChanged.emit()

if __name__ == "__main__":
    import doctest
    doctest.testmod()
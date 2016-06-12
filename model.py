from collections import OrderedDict
import operator
from PyQt4.QtCore import *

from query_manager import QueryManager

class AbstractModel(QAbstractTableModel):
    filters_changed = pyqtSignal()
    model_error = pyqtSignal(str)

    def __init__(self):
        super(AbstractModel, self).__init__()
        self.query_manager = QueryManager()
        self._original_data = []
        self._modified_data = []
        self._header = self.query_manager.headers
        self.query_manager.query_error.connect(self.query_errored)

    @property
    def totals(self) -> dict:
        try:
            totals = OrderedDict()
            for i, fld in self.query_manager.fields.items():
                if fld.type == 'float':
                    total = sum([val[i] for val in self._modified_data])
                    avg = total/self.rowCount() if self.rowCount() > 0 else 0
                    totals[fld.name + ' Sum'] = '{0:.2f}'.format(float(total))
                    totals[fld.name + ' Avg'] = '{0:.2f}'.format(float(avg))
                elif fld.type == 'date':
                    minimum = min([val[i] for val in self._modified_data] or [0])
                    maximum = max([val[i] for val in self._modified_data] or [0])
                    totals[fld.name + ' Min'] = str(minimum)
                    totals[fld.name + ' Max'] = str(maximum)
                else:
                    totals[fld.name + ' DCount'] = str(len(set([val[i] for val in self._modified_data])))
            return totals
        except Exception as e:
            self.model_error.emit(str(e))
            return {}

    def rowCount(self, parent=None):
        return len(self._modified_data) if self._modified_data else 0

    def columnCount(self, parent=None):
        return len(self._modified_data[0]) if self._modified_data else 0

    def data(self, index, role):
        try:
            if not index.isValid():
                return
            elif role != Qt.DisplayRole:
                return
            return self._modified_data[index.row()][index.column()]
        except Exception as e:
            self.model_error.emit(str(e))

    def filter_equality(self, col_ix, val):
        self._modified_data = [x for x in self._modified_data if x[col_ix] == val]
        self.filters_changed.emit()

    def filter_greater_than(self, col_ix, val):
        self._modified_data = [x for x in self._modified_data if x[col_ix] >= val]
        self.filters_changed.emit()

    def filter_less_than(self, col_ix, val):
        self._modified_data = [x for x in self._original_data if x[col_ix] <= val]
        self.filters_changed.emit()

    def filter_like(self, val):
        self.layoutAboutToBeChanged.emit()
        self._modified_data = []
        for row in self._original_data:
            for itm in row:
                if itm and val.lower() in str(itm).lower():
                    self._modified_data.append(row)
                    break
        self.layoutChanged.emit()
        self.filters_changed.emit()

    def full_reset(self):
        self.layoutAboutToBeChanged.emit()
        self._original_data = []
        self._modified_data = []
        self.layoutChanged.emit()
        self.filters_changed.emit()

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._header[col]
        return None

    @property
    def header(self):
        return self._header

    def pull(self):
        try:
            self.layoutAboutToBeChanged.emit()
            self._original_data = self.query_manager.results()
            self._modified_data = self._original_data
            self.filters_changed.emit()
            self.layoutChanged.emit()
        except Exception as e:
            self.model_error.emit(str(e))

    @pyqtSlot(str)
    def query_errored(self, msg):
        self.model_error.emit(msg)

    def reset(self):
        self.layoutAboutToBeChanged.emit()
        self._modified_data = self._original_data
        self.filters_changed.emit()
        self.layoutChanged.emit()

    def row(self, row, parent=None):
        """
        This method returns a list of values given a row number.
        It is required by the custom sort filter proxy model used by the table.
        """
        values = []
        record = self.record(row)
        for i in range(record.count()):
            values.append(record.value(i))
        return values

    def sort(self, col, order):
        """sort table by given column number col"""
        try:
            self.layoutAboutToBeChanged.emit()
            self._modified_data = sorted(
                self._modified_data
                , key=operator.itemgetter(col)
            )
            if order == Qt.DescendingOrder:
                self._modified_data.reverse()
            self.layoutChanged.emit()
        except Exception as e:
            self.model_error(str(e))
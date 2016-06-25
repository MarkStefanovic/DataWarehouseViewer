from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import operator
import re
import threading

from PyQt4 import QtCore

from export_sql import ExportSql
from messenger import global_message_queue
from query_manager import QueryManager
from utilities import is_float


class AbstractModel(QtCore.QAbstractTableModel):
    filters_changed_signal = QtCore.pyqtSignal()
    model_error_signal = QtCore.pyqtSignal(str)
    rows_returned_signal = QtCore.pyqtSignal(str)
    totals_changed_signal = QtCore.pyqtSignal(dict)
    rows_exported_signal = QtCore.pyqtSignal(int)

    def __init__(self, config):
        super(AbstractModel, self).__init__()
        self._query = QueryManager(config)
        self._original_data = []
        self._modified_data = []
        self._header = self._query.headers
        self._max_rows = self._query._max_rows

        self.exporter = ExportSql()

        #   connect signals
        self._query.query_error_signal.connect(self.query_errored)
        self._query.query_results_signal.connect(self.update_view)
        self._query.rows_returned_signal.connect(self.rows_returned_signal.emit)  # pass along
        self.filters_changed_signal.connect(self.calculate_totals)
        self.exporter.signals.rows_exported.connect(self.rows_exported_signal.emit)  # pass along

    def calculate_totals(self) -> dict:
        threading.Thread(target=self.calculate_totals_thread).start()

    def calculate_totals_thread(self):
        totals = {}
        for i, fld in self._query.fields.items():
            if fld.type == 'float':
                total = sum([val[i] for val in self._modified_data if
                    is_float(val[i])])
                avg = total / self.rowCount() if self.rowCount() > 0 else 0
                totals['{} Sum'.format(fld.name)] = '{:,.2f}'.format(
                    float(total))
                totals['{} Avg'.format(fld.name)] = '{:,.2f}'.format(
                    float(avg))
            elif fld.type == 'date':
                minimum = min(
                    [val[i] for val in self._modified_data] or [0])
                maximum = max(
                    [val[i] for val in self._modified_data] or [0])
                totals['{} Min'.format(fld.name)] = str(minimum)
                totals['{} Max'.format(fld.name)] = str(maximum)
            else:
                totals['{} DCount'.format(fld.name)] = str(
                    len(set([val[i] for val in self._modified_data])))
        valid_totals = OrderedDict({key: val for key, val in totals.items() if val})
        self.totals_changed_signal.emit(valid_totals)

        # with ThreadPoolExecutor(max_workers=1) as ex:
        #     ex.submit(calc_tot)

    def export(self):
        self.exporter.start_pull(self._query.sql_export, self._query._db)

    def rowCount(self, parent=None):
        return len(self._modified_data) if self._modified_data else 0

    def columnCount(self, parent=None):
        return len(self._modified_data[0]) if self._modified_data else 0

    def data(self, index, role):
        # TODO: allow user to specify format in config file
        def formatter(value, fmt):
            format_options = {
                'currency': '${:,.2f}'
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

        col_type = self._query.fields.get(index.column()).type
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
                return val
        except Exception as e:
            err_msg = 'Error modeling data: {}'.format(e)
            global_message_queue.errored(err_msg)
            # self.model_error_signal.emit(err_msg)

    def distinct_values(self, col_ix):
        return sorted(set([val[col_ix] for val in self._modified_data]))

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
        self._modified_data = [x for x in self._original_data if str(x[col_ix]) in [y for y in values]]
        self.filters_changed_signal.emit()

    def full_reset(self):
        self.layoutAboutToBeChanged.emit()
        self._original_data = []
        self._modified_data = []
        self.layoutChanged.emit()
        self.filters_changed_signal.emit()

    def headerData(self, col, orientation, role):
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return self._header[col]
        return None

    @property
    def header(self):
        return self._header

    def pull(self):
        self._query.pull()

    @QtCore.pyqtSlot(str)
    def query_errored(self, msg):
        self.model_error_signal.emit(msg)

    def reset(self):
        self.layoutAboutToBeChanged.emit()
        self._modified_data = self._original_data
        self.filters_changed_signal.emit()
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
            if order == QtCore.Qt.DescendingOrder:
                self._modified_data.reverse()
            self.layoutChanged.emit()
        except Exception as e:
            self.model_error_signal(str(e))

    @QtCore.pyqtSlot(str)
    def update_view(self, results):
        try:
            self.layoutAboutToBeChanged.emit()
            self._original_data = results
            self._modified_data = results
            self.filters_changed_signal.emit()
            self.layoutChanged.emit()
        except Exception as e:
            self.query_errored.emit(str(e))

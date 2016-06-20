"""This module displays the data provided by the query manager"""

from collections import namedtuple, OrderedDict
from functools import partial
import os
from subprocess import Popen

from PyQt4 import QtCore, QtGui
import xlwt

from json_config import SimpleJsonConfig
from model import AbstractModel
from utilities import timestr


class DatasheetView(QtGui.QWidget):
    """
    This class takes a model as an input and creates an editable datasheet therefrom.
    """

    def __init__(self, config, parent=None):
        super(DatasheetView, self).__init__()
        self.setWindowFlags = (
            QtCore.Qt.WindowMinimizeButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint
        )
        self.model = AbstractModel(config=config)
        self.table = QtGui.QTableView()
        self.table.setSortingEnabled(True)
        self.query_controls = {}
        self.menu = QtGui.QMenu(self)

        self.table.setModel(self.model)

        self.table.resizeColumnsToContents()
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table.setShowGrid(False)

    #   CREATE WIDGETS
        self.top_button_box = QtGui.QDialogButtonBox()
        self.btn_reset = QtGui.QPushButton('&Reset')
        self.txt_search = QtGui.QLineEdit()
        self.lbl_search = QtGui.QLabel('Quick Search:')

    #   LAYOUT
        self.layout = QtGui.QGridLayout()
        self.setLayout(self.layout)
        self.layout.setColumnStretch(0, 1)
        self.layout.setColumnStretch(1, 6)

        self.qry_layout = self.query_layout()
        self.layout.addLayout(self.qry_layout, 0, 0, 1, 1, QtCore.Qt.AlignTop)

        ds_layout = QtGui.QGridLayout()
        ds_layout.setColumnStretch(3, 1)
        ds_layout.setColumnStretch(2, 4)
        ds_layout.addWidget(self.btn_reset, 0, 0, 1, 1)
        ds_layout.addWidget(self.lbl_search, 0, 1, 1, 1)
        ds_layout.addWidget(self.txt_search, 0, 2, 1, 1)
        ds_layout.addWidget(self.table, 1, 0, 1, 3)
        self.summary = self.summary_frame()
        ds_layout.addWidget(self.summary, 0, 3, 2, 1)
        self.layout.addLayout(ds_layout, 0, 1, 1, 1, QtCore.Qt.AlignTop)

        bottom_bar = QtGui.QGridLayout()
        self.statusbar = QtGui.QStatusBar()
        self.statusbar.showMessage("")
        bottom_bar.addWidget(self.statusbar, 0, 0, 1, 4)
        self.layout.addLayout(bottom_bar, 1, 0, 1, 2)

    #   CONNECT SIGNALS
        self.model.filters_changed_signal.connect(self.refresh_summary)
        self.model.model_error_signal.connect(self.outside_error)
        self.txt_search.textChanged.connect(self.on_lineEdit_textChanged)
        self.btn_reset.clicked.connect(self.reset)
        # self.model.filters_changed_signal.connect(self.reset_status)
        self.model.filters_changed_signal.connect(self.table.resizeColumnsToContents)
        self.model.rows_returned_signal.connect(self.show_rows_returned)

    def get_all_selected_ids(self):
        """ returns the selected primary key of the selected row """
        selection_model = self.table.selectionModel()
        selected_indexes = selection_model.selectedIndexes()
        selected_ids = []
        for index in selected_indexes:
            proxy_index = self.proxy.mapToSource(index)
            row = proxy_index.row()
            ix = self.model.index(row, 0)  # assumes the id column is column 0
            id = self.model.data(ix, QtCore.Qt.DisplayRole)
            selected_ids.append(id)
        return selected_ids

    def exit(self):
        self.stop_everything.emit()

    # def export_results(self) -> None:
    #     self.to_excel(data=self.model._original_data, header=self.model.query.headers)
    #
    def export_visible(self) -> None:
        self.to_excel(data=self.model._modified_data, header=self.model.query.headers)

    @QtCore.pyqtSlot(int)
    def filter_col_like(self, col_ix):
        self.model.filter_like(val=self.col_like.text(), col_ix=col_ix)

    def set_status(self, msg, duration=None) -> None:
        if duration:
            self.statusbar.showMessage(msg, duration)
        else:
            self.statusbar.showMessage(msg)

    def keyPressEvent(self, event):
        if event.matches(QtGui.QKeySequence.Copy):
            self.copy()
        else:
            super(DatasheetView, self).keyPressEvent(event)

    @QtCore.pyqtSlot()
    def rclick_menu_delete(self):
        ids = self.get_all_selected_ids()
        self.model.delete_records(ids)

    def reset(self):
        self.table.resizeColumnsToContents()
        self.txt_search.setText('')
        self.model.reset()

    @QtCore.pyqtSlot(str)
    def on_lineEdit_textChanged(self, text):
        self.model.filter_like(self.txt_search.text())

    def copy(self):
        """Copy selected cells into copy-buffer"""
        selection = self.table.selectionModel()
        indexes = selection.selectedIndexes()

        rows = OrderedDict()
        for idx in indexes:
            row = idx.row()
            rows[row] = rows.get(row, [])
            rows[row].append(str(idx.data()) or '')
        str_array = ''
        for row in rows.keys():
            str_array += '\t'.join([col for col in rows[row]])
            str_array += '\n'
        QtGui.QApplication.clipboard().setText(str_array)

    def pull(self):
        self.set_status("{}: Pulling {}".format(timestr(), self.model.query.str_criteria))
        self.model.pull()

    def contextMenuEvent(self, event):
        """Implements right-clicking on cell."""
        if not self.table.underMouse():
            return

        rows = sorted(set(index.row() for index in self.table.selectedIndexes()))
        cols = sorted(set(index.column() for index in self.table.selectedIndexes()))

        row_ix = rows[0] if rows else 0
        col_ix = cols[0] if cols else 0
        if row_ix < 0 or col_ix < 0:
            return  # out of bounds

        menu = QtGui.QMenu(self)

        menu = self.make_cell_context_menu(menu, row_ix, col_ix)
        menu.exec_(self.mapToGlobal(event.pos()))

    def make_cell_context_menu(self, menu, row_ix, col_ix):
        """Create the mneu displayed when right-clicking on a cell."""
        try:
            val = self.model._modified_data[row_ix][col_ix]
        except IndexError:
            val = ""

    #   Text Search Box: add input box to search for values in the column containing the search term
        self.col_like = QtGui.QLineEdit()
        self.col_like.setPlaceholderText("Text like")
        txt_wac = QtGui.QWidgetAction(menu)
        txt_wac.setDefaultWidget(self.col_like)
        menu.addAction(txt_wac)
        self.col_like.textChanged.connect(partial(self.filter_col_like, col_ix))
        menu.addSeparator()

    #   List Box: show distinct values for column
        self.list = QtGui.QListWidget()
        self.list.setFixedHeight(100)
        lst_wac = QtGui .QWidgetAction(menu)
        lst_wac.setDefaultWidget(self.list)
        menu.addAction(lst_wac)

        show_all = QtGui.QListWidgetItem("Show All")
        show_all.setFlags(show_all.flags() | QtCore.Qt.ItemIsUserCheckable)
        show_all.setCheckState(QtCore.Qt.Checked)
        self.list.addItem(show_all)
        for item in self.model.distinct_values(col_ix):
            i = QtGui.QListWidgetItem('%s' % item)
            i.setFlags(i.flags() | QtCore.Qt.ItemIsUserCheckable)
            i.setCheckState(QtCore.Qt.Checked)
            self.list.addItem(i)
        self.list.itemChanged.connect(partial(self.on_list_selection_changed, col_ix=col_ix))
        menu.addSeparator()

        menu.addAction(
            "Show Equal To"
            , partial(
                self.model.filter_equality
                , col_ix=col_ix
                , val=val
            )
        )
        menu.addAction(
            "Show Greater Than or Equal To"
            , partial(
                self.model.filter_greater_than
                , col_ix=col_ix
                , val=val
            )
        )
        menu.addAction(
            "Show Less Than or Equal To"
            , partial(
                self.model.filter_less_than
                , col_ix=col_ix
                , val=val
            )
        )
        menu.addSeparator()

        menu.addAction(
            "Reset filters"
            , self.model.reset
        )
        menu.addSeparator()

        menu.addAction("Open in Excel", self.export_visible)
        return menu

    def on_list_selection_changed(self, item, col_ix):
        # self.list.blockSignals(True)
        all_items = [self.list.item(i) for i in range(len(self.list))]
        check_state = {
            itm.text(): itm.checkState()
            for itm in all_items
        }
        if item.text() == "Show All" and item.checkState() == QtCore.Qt.Checked:
            [itm.setCheckState(QtCore.Qt.Checked) for itm in all_items if itm.text() != "Show All"]
        elif item.text() != "Show All" and item.checkState() == QtCore.Qt.Unchecked:
            all_items[0].setCheckState(QtCore.Qt.Unchecked)

        if check_state.get("Show All") != QtCore.Qt.Checked:
            checked_values = set(str(itm.text()) for itm in all_items if itm.checkState() == QtCore.Qt.Checked)
            self.model.filter_set(col_ix, checked_values)
        # self.list.blockSignals(False)

    @QtCore.pyqtSlot(str)
    def outside_error(self, msg):
        self.set_status(msg)

    # def open_config_file(self):
    #     Popen('config.json', shell=True)

    def to_excel(self, data, header):
        """Save displayed items to Excel file."""

        if not data:
            self.set_status('No data was returned')
            return
        folder = 'output'
        if not os.path.exists(folder) or not os.path.isdir(folder):
            os.mkdir(folder)
        wb = xlwt.Workbook()
        sht = wb.add_sheet('temp', cell_overwrite_ok=True)
        header_style = xlwt.easyxf(
            'pattern: pattern solid, fore_colour dark_blue;'
            'font: colour white, bold True;'
        )
        [sht.write(0, i, x, header_style) for i, x in enumerate(header)]
        for i, row in enumerate(data):
            for j, col in enumerate(data[i]):
                sht.write(i + 1, j, col)
        dest = os.path.join(folder, 'temp.xls')
        wb.save(dest)
        Popen(dest, shell=True)

    def add_query_criteria(self, name, text, type):
        self.model.query.add_criteria(field_name=name, value=text(), field_type=type)

    def reset_query(self):
        for key, val in self.query_controls.items():
            val.handle.setText('')
        self.txt_search.setText('')
        self.model.query.reset()
        self.model.full_reset()
        self.set_status("")

    def hide_query_designer(self):
        self.layout.removeItem(self.qry_layout)
        if self.qry_layout is not None:
            while self.qry_layout.count():
                item = self.qry_layout.takeAt(0)
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
                else:
                    self.clearLayout(item.layout())
        self.qry_layout.deleteLater()
        self.layout.setColumnStretch(0, 0)

    def show_query_designer(self):
        self.qry_layout = self.query_layout()
        self.layout.addLayout(self.qry_layout, 0, 0, 1, 1, QtCore.Qt.AlignTop)
        self.layout.setColumnStretch(0, 1)

    @QtCore.pyqtSlot(str)
    def show_rows_returned(self, msg):
        self.set_status('{}'.format(msg))

    def query_layout(self):
        """Populate a layout with filter controls"""
        layout = QtGui.QGridLayout()
        Control = namedtuple('Control', 'handle, type')
        current_row = 0

        def add_row(name: str, type: str, label: str=None) -> None:
            nonlocal current_row
            lbl = QtGui.QLabel(label or name)
            txt = QtGui.QLineEdit()
            ctrl_name = '{name}_{type}'.format(name=name, type=type)
            self.query_controls[ctrl_name] = Control(handle=txt, type=type)
            layout.addWidget(lbl, current_row, 0, 1, 1)
            layout.addWidget(txt, current_row, 1, 1, 1)

            params = (name, txt.text, type)
            cmd = lambda p=params, func=self.add_query_criteria: func(*params)
            txt.textChanged.connect(cmd)

            current_row += 1

        def date_handler(name: str) -> None:
            add_row(name, 'date_start', label='{} Start'.format(name))
            add_row(name, 'date_end', label='{} End'.format(name))

        def float_handler(name):
            add_row(name, 'float')

        def int_handler(name):
            add_row(name, 'int')

        def str_handler(name):
            add_row(name, 'str', label='{} Like'.format(name))

        handlers = {
            'date':    date_handler
            , 'float': float_handler
            , 'int':   int_handler
            , 'str':   str_handler
        }

        for field_name, field_type in self.model.query.filter_options[:10]:
            handlers.get(field_type)(field_name)

        self.btn_reset_query = QtGui.QPushButton('&Reset Query')
        self.btn_reset_query.clicked.connect(self.reset_query)
        layout.addWidget(self.btn_reset_query, current_row, 0, 1, 1)

        self.btn_pull = QtGui.QPushButton('&Pull')
        self.btn_pull.clicked.connect(self.pull)
        layout.addWidget(self.btn_pull, current_row, 1, 1, 1)

        return layout

    @QtCore.pyqtSlot()
    def refresh_summary(self):
        for i, (key, val) in enumerate(self.model.totals.items()):
            measure = QtGui.QTableWidgetItem(key)
            value = QtGui.QTableWidgetItem(val)
            value.setTextAlignment(QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter)
            self.summary.setItem(i, 0, measure)
            self.summary.setItem(i, 1, value)
        self.summary.resizeColumnsToContents()

    def reset_status(self):
        self.statusbar.showMessage("")

    def summary_frame(self):
        tbl = QtGui.QTableWidget()

        tbl.setRowCount(len(self.model.totals))
        tbl.setColumnCount(2)
        tbl.setAlternatingRowColors(True)
        for i, (key, val) in enumerate(self.model.totals.items()):
            measure = QtGui.QTableWidgetItem(key)
            value = QtGui.QTableWidgetItem(val)
            value.setTextAlignment(QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter)
            tbl.setItem(i, 0, measure)
            tbl.setItem(i, 1, value)
        tbl.setHorizontalHeaderLabels(['Measure', 'Value'])
        tbl.resizeColumnsToContents()
        return tbl

class MainView(QtGui.QDialog):
    def __init__(self, parent=None):
        super(MainView, self).__init__(parent
            , QtCore.Qt.WindowMinimizeButtonHint | QtCore.Qt.WindowMaximizeButtonHint)

        app_config = SimpleJsonConfig(os.path.join('config', 'app.json'))
        app_name = app_config.get_or_set_variable('app_name', 'TestApp')
        datasheets = app_config.get_or_set_variable(
            'datasheets', {
                'Customers': os.path.join('config', 'customer_config.json')
                , 'Customers2': os.path.join('config', 'customer_config2.json')
            }
        )

        self.setWindowTitle(app_name)

        tabs = QtGui.QTabWidget()
        for name, config in sorted(datasheets.items()):
            tabs.addTab(DatasheetView(config=config), name)

        mainLayout = QtGui.QVBoxLayout()
        self.menubar = QtGui.QMenuBar()
        mainLayout.addWidget(self.menubar)
        filemenu = self.menubar.addMenu('&File')
        filemenu.addAction('Open Output Folder', self.open_output_folder)
        # filemenu.addAction('Open Settings File', ds.open_config_file)
        # filemenu = self.menubar.addMenu('&View')
        # filemenu.addAction('Show Query Designer', ds.show_query_designer)
        # filemenu.addAction('Hide Query Designer', ds.hide_query_designer)
        # filemenu = self.menubar.addMenu('&Export')
        # filemenu.addAction('Export Visible', ds.export_visible)
        # filemenu.addAction('Export Results', ds.export_results)

        mainLayout.addWidget(tabs)
        self.setLayout(mainLayout)

    def open_output_folder(self):
        folder = SimpleJsonConfig().get_or_set_variable('output_folder', 'output')
        if not os.path.exists(folder) or not os.path.isdir(folder):
            os.mkdir(folder)
        os.startfile(folder)

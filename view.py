"""This module displays the data provided by the query manager"""
from collections import namedtuple, OrderedDict
import datetime
from functools import partial
import os
from pathlib import Path
from subprocess import Popen
import sys

from PyQt4 import QtCore, QtGui, QtSql
from PyQt4.QtCore import pyqtSlot as Slot
import xlwt

from config import SimpleJsonConfig
from model import AbstractModel


class DatasheetView(QtGui.QWidget):
    """
    This class takes a model as an input and creates
    an editable datasheet therefrom
    """

    def __init__(self, parent=None):
        super(DatasheetView, self).__init__()
        self.setWindowFlags = (
            QtCore.Qt.WindowMinimizeButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint
        )
        self.setWindowTitle('PeopleNet')
        self.model = AbstractModel()
        self.query = self.model.query_manager  # shortcut
        self.setWindowTitle('PeopleNet')
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
        self.model.filters_changed.connect(self.refresh_summary)
        self.model.model_error.connect(self.outside_error)
        self.txt_search.textChanged.connect(self.on_lineEdit_textChanged)
        self.btn_reset.clicked.connect(self.reset)

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

    def export_results(self) -> None:
        self.to_excel(data=self.model._original_data, header=self.query.headers)

    def export_visible(self) -> None:
        self.to_excel(data=self.model._modified_data, header=self.query.headers)

    def set_status(self, msg, duration=5000):
        self.statusbar.showMessage(msg, duration)

    def keyPressEvent(self, event):
        if event.matches(QtGui.QKeySequence.Copy):
            self.copy()
        else:
            super(DatasheetView, self).keyPressEvent(event)

    @Slot()
    def rclick_menu_delete(self):
        ids = self.get_all_selected_ids()
        self.model.delete_records(ids)

    def reset(self):
        self.txt_search.setText('')
        self.model.reset()

    @Slot(str)
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
        self.set_status("Pulling data from the server...")
        self.query.pull()
        self.reset_status()
        self.table.resizeColumnsToContents()

    def contextMenuEvent(self, event):
        """Implements right-clicking on cell."""
        rows = sorted(set(index.row() for index in self.table.selectedIndexes()))
        cols = sorted(set(index.column() for index in self.table.selectedIndexes()))
        # for row in rows:
        #     print('Rows {} is selected'.format(rows))
        #     print('Columns {} is selected'.format(cols))

        row_ix = rows[0] if rows else 0
        col_ix = cols[0] if cols else 0
        if row_ix < 0 or col_ix < 0:
            return  # out of bounds

        menu = QtGui.QMenu(self)
        menu = self.make_cell_context_menu(menu, row_ix, col_ix)
        menu.exec_(self.mapToGlobal(event.pos()))

    def make_cell_context_menu(self, menu, row_ix, col_ix):
        """Create the mneu displayed when right-clicking on a cell."""
        val = self.model._modified_data[row_ix][col_ix]

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

    @Slot(str)
    def outside_error(self, msg):
        self.set_status(msg)

    def open_output_folder(self):
        folder = SimpleJsonConfig().get_or_set_variable('output_folder', 'output')
        if not os.path.exists(folder) or not os.path.isdir(folder):
            os.mkdir(folder)
        os.startfile(folder)

    def open_config_file(self):
        Popen('config.json', shell=True)

    def to_excel(self, data, header):
        """Save displayed items to Excel file."""

        if not data:
            self.set_status('No data was returned')
            return
        folder = SimpleJsonConfig().get_or_set_variable('output_folder', 'output')
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
        self.query.add_criteria(field_name=name, value=text(), field_type=type)

    def reset_query(self):
        for key, val in self.query_controls.items():
            val.handle.setText('')
        self.txt_search.setText('')
        self.query.reset()
        self.model.full_reset()

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

    def query_layout(self):
        """Populate a layout with filter controls"""
        layout = QtGui.QGridLayout()
        Control = namedtuple('Control', 'handle, type')
        current_row = 0

        def add_row(name: str, type: str, label: str=None) -> None:
            nonlocal current_row
            lbl = QtGui.QLabel(label or name)
            txt = QtGui.QLineEdit()
            self.query_controls[name + '_' + type] = Control(handle=txt, type=type)
            layout.addWidget(lbl, current_row, 0, 1, 1)
            layout.addWidget(txt, current_row, 1, 1, 1)

            params = (name, txt.text, type)
            cmd = lambda p=params, func=self.add_query_criteria: func(*params)
            txt.textChanged.connect(cmd)

            current_row += 1

        def date_handler(name: str) -> None:
            add_row(name, 'date_start', label=name + ' Start')
            add_row(name, 'date_end', label=name + ' End')

        def float_handler(name):
            add_row(name, 'float')

        def int_handler(name):
            add_row(name, 'int')

        def str_handler(name):
            add_row(name, 'str', label=name + ' Like')

        handlers = {
            'date':    date_handler
            , 'float': float_handler
            , 'int':   int_handler
            , 'str':   str_handler
        }

        for field_name, field_type in self.query.filter_options[:10]:
            handlers.get(field_type)(field_name)

        self.btn_reset_query = QtGui.QPushButton('&Reset Query')
        self.btn_reset_query.clicked.connect(self.reset_query)
        layout.addWidget(self.btn_reset_query, current_row, 0, 1, 1)

        self.btn_pull = QtGui.QPushButton('&Pull')
        self.btn_pull.clicked.connect(self.model.pull)
        layout.addWidget(self.btn_pull, current_row, 1, 1, 1)

        return layout

    @Slot()
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

        # tbl.setHorizontalHeaderItem(0, QtGui.QTableWidgetItem('Measure'))
        # tbl.setHorizontalHeaderItem(0, QtGui.QTableWidgetItem('Val'))
        # tbl.setHorizontalHeaderLabels('Measure, Value')
        tbl.setRowCount(len(self.model.totals))
        tbl.setColumnCount(2)
        tbl.setAlternatingRowColors(True)
        # new_item.setTextAlignment(QtCore.Qt.AlignHCenter | QtCore.Qt.AlignVCenter)
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

        tabs = QtGui.QTabWidget()
        ds = DatasheetView()
        tabs.addTab(ds, 'PeopleNet')
        # tabs.addTab(DatasheetView(), 'Books2')

        mainLayout = QtGui.QVBoxLayout()

        #   change the background color to blue
        palette = self.palette()
        palette.setColor(self.backgroundRole(), QtGui.QColor(30, 80, 140))
        palette.setColor(self.foregroundRole(), QtGui.QColor(0, 0, 0))
        self.setPalette(palette)

        self.menubar = QtGui.QMenuBar()
        mainLayout.addWidget(self.menubar)
        filemenu = self.menubar.addMenu('&File')
        filemenu.addAction('Open Output Folder', ds.open_output_folder)
        filemenu.addAction('Open Settings File', ds.open_config_file)
        filemenu = self.menubar.addMenu('&View')
        filemenu.addAction('Show Query Designer', ds.show_query_designer)
        filemenu.addAction('Hide Query Designer', ds.hide_query_designer)
        filemenu = self.menubar.addMenu('&Export')
        filemenu.addAction('Export Visible', ds.export_visible)
        filemenu.addAction('Export Results', ds.export_results)

        mainLayout.addWidget(tabs)
        self.setLayout(mainLayout)
        self.setWindowTitle("Main")

    def filemenu_placeholder_action(self):
        print('filemenu_placeholder_action pressed')

if __name__ == '__main__':
    try:
        app = QtGui.QApplication(sys.argv)

        # app.setStyle('cleanlooks')
        app.setStyle("plastique")
        with open('darkcity.css', 'r') as fh:
            style_sheet = fh.read()
        app.setStyleSheet(style_sheet)
        #   set font
        font = QtGui.QFont("Arial", 11)
        app.setFont(font)

        icon = QtGui.QIcon('app.ico')
        app.setWindowIcon(icon)

        main_view = MainView()
        main_view.showMaximized()
        main_view.setWindowTitle('PeopleNet')
        app.exec_()
        # sys.exit(0)
        os._exit(0)
    except SystemExit:
        print("Closing Window...")
    except Exception as e:
        print(sys.exc_info()[1])
        # logger.error(e)
        sys.exit(1)

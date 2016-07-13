"""This module displays the data provided by the query manager"""

from collections import namedtuple, OrderedDict
from functools import partial
import os
from subprocess import Popen
from typing import Any, Dict, List, Set, Tuple

from PyQt4 import QtCore, QtGui
import xlwt

from json_config import SimpleJsonConfig
from logger import log_error
from model import AbstractModel
from query_manager import Field, Filter, query_manager_config
from utilities import rootdir, timestr

Control = namedtuple('Control', 'handle type')
Criteria = namedtuple('Criteria', 'field_name value field_type operator')


class DatasheetView(QtGui.QWidget):
    """
    This class takes a model as an input and creates an editable datasheet therefrom.
    """
    exit_signal = QtCore.pyqtSignal()

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
        self.layout.setColumnStretch(0, 1)  # query designer
        self.layout.setColumnStretch(1, 6)  # ds and summary

        self.query_designer = QueryDesigner(
            filters=self.model._query_manager.filters
            , max_display_rows=self.model._query_manager.max_display_rows
            , max_export_rows=self.model._query_manager.max_export_rows
        )
        self.layout.addWidget(self.query_designer, 0, 0, 1, 1, QtCore.Qt.AlignTop)

        ds_layout = QtGui.QGridLayout()
        ds_layout.setColumnStretch(2, 4)
        ds_layout.addWidget(self.btn_reset, 0, 0, 1, 1)
        ds_layout.addWidget(self.lbl_search, 0, 1, 1, 1)
        ds_layout.addWidget(self.txt_search, 0, 2, 1, 1)
        ds_layout.addWidget(self.table, 1, 0, 1, 3)
        self.layout.addLayout(ds_layout, 0, 1, 1, 1, QtCore.Qt.AlignTop)

        bottom_bar = QtGui.QGridLayout()
        self.statusbar = QtGui.QStatusBar()
        self.statusbar.showMessage("")
        bottom_bar.addWidget(self.statusbar, 0, 0, 1, 4)
        self.layout.addLayout(bottom_bar, 1, 0, 1, 2)

    #   CONNECT SIGNALS
        self.exit_signal.connect(self.model.exit_signal.emit)
        self.txt_search.textChanged.connect(self.on_lineEdit_textChanged)
        self.btn_reset.clicked.connect(self.reset)
        self.model.error_signal.connect(self.outside_error)
        self.model.layoutChanged.connect(self.table.resizeColumnsToContents)
        self.model.rows_exported_signal.connect(self.show_rows_exported)
        self.model.rows_returned_signal.connect(self.show_rows_returned)
        self.query_designer.add_criteria_signal.connect(self.add_query_criteria)
        self.query_designer.error_signal.connect(self.set_status)
        self.query_designer.export_signal.connect(self.export_all)
        self.query_designer.pull_signal.connect(self.pull)
        self.query_designer.reset_signal.connect(self.reset_query)
        self.query_designer.stop_export_signal.connect(
            self.model._query_manager._exporter.signals.exit.emit)

    def add_query_criteria(self, criteria: Criteria):
        field = Field(name=criteria.field_name, type=criteria.field_type)
        self.model._query_manager.add_criteria(
            field=field
            , value=criteria.value
            , operator=criteria.operator
        )

    def exit(self):
        self.stop_everything.emit()

    def export_all(self) -> None:
        self.set_status('Exporting top 500K rows')  # TODO: change to config variable at app level
        self.model.export()

    def export_visible(self) -> None:
        self.to_excel(data=self.model._modified_data, header=self.model._query_manager.headers)

    @QtCore.pyqtSlot(int)
    def filter_col_like(self, col_ix):
        self.model.filter_like(val=self.col_like.text(), col_ix=col_ix)

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

    def hide_query_designer(self):
        self.layout.removeItem(self.query_designer)
        if self.query_designer is not None:
            while self.query_designer.count():
                item = self.query_designer.takeAt(0)
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
                else:
                    self.clearLayout(item.layout())
        self.query_designer.deleteLater()
        self.layout.setColumnStretch(0, 0)

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
        self.set_status("{}: Pulling {}".format(timestr(), self.model._query_manager.str_criteria))
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

        def add_item(text, check_state=QtCore.Qt.Checked):
            i = QtGui.QListWidgetItem('%s' % text)
            i.setFlags(i.flags() | QtCore.Qt.ItemIsUserCheckable)
            i.setCheckState(check_state)
            self.list.addItem(i)

        add_item("Show All")
        add_item("None", QtCore.Qt.Unchecked)
        [add_item(itm) for itm in self.model.distinct_values(col_ix)]
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

        menu.addSeparator()
        submenu = QtGui.QMenu(menu)
        submenu.setTitle("Summary Stats")
        menu.addMenu(submenu)
        for itm in self.model.field_totals(col_ix):
            submenu.addAction(itm)

        return menu

    def on_list_selection_changed(self, item, col_ix):
        all_items = [self.list.item(i) for i in range(len(self.list))]

        Item = namedtuple('Items', 'handle check_state')
        items_dict = {
            itm.text(): Item(handle=itm, check_state=itm.checkState())
            for itm in all_items
        }

        show_all_item = items_dict.get("Show All").handle
        none_item = items_dict.get("None").handle
        checked_items = [itm for itm in all_items if not itm in [show_all_item, none_item] and itm.checkState() == QtCore.Qt.Checked]
        unchecked_items = [itm for itm in all_items if not itm in [show_all_item, none_item] and itm.checkState() == QtCore.Qt.Unchecked]
        item_checked = True if item.checkState() == QtCore.Qt.Checked else False

        def add_one():
            none_item.setCheckState(QtCore.Qt.Unchecked)
            if not unchecked_items:
                show_all_item.setCheckState(QtCore.Qt.Checked)

        def remove_one():
            show_all_item.setCheckState(QtCore.Qt.Unchecked)
            if not checked_items:
                none_item.setCheckState(QtCore.Qt.Checked)

        def show_all():
            [itm.setCheckState(QtCore.Qt.Checked) for itm in all_items if itm != show_all_item]
            none_item.setCheckState(QtCore.Qt.Unchecked)

        def show_none():
            [itm.setCheckState(QtCore.Qt.Unchecked) for itm in all_items if itm != none_item]
            show_all_item.setCheckState(QtCore.Qt.Unchecked)

        self.list.blockSignals(True)
        if item == show_all_item:
            if item_checked:
                show_all()
        elif item == none_item:
            if item_checked:
                show_none()
        else:
            if item_checked:
                add_one()
            else:
                remove_one()
        self.list.blockSignals(False)

        checked_values = set(
            str(itm.text())
            for itm in all_items
            if itm.checkState() == QtCore.Qt.Checked
               and itm not in [show_all_item, none_item]
        )
        self.model.filter_set(col_ix, checked_values)


    @QtCore.pyqtSlot(str)
    def outside_error(self, msg):
        self.set_status(msg)

    # def open_config_file(self):
    #     Popen('config.json', shell=True)09

    def to_excel(self, data, header):
        """Save displayed items to Excel file."""

        if not data:
            # self.set_status('No data was returned')
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

    def reset_query(self):
        self.txt_search.setText('')
        self.model._query_manager.reset()
        self.model.full_reset()
        self.set_status("")

    def set_status(self, msg, duration=None) -> None:
        if duration:
            self.statusbar.showMessage(msg, duration)
        else:
            self.statusbar.showMessage(msg)

    def show_query_designer(self):
        self.query_designer = self.query_layout()
        self.layout.addLayout(self.query_designer, 0, 0, 1, 1, QtCore.Qt.AlignTop)
        self.layout.setColumnStretch(0, 1)

    @QtCore.pyqtSlot(int)
    def show_rows_exported(self, msg):
        self.set_status('Rows exported {}...'.format(msg))

    @QtCore.pyqtSlot(str)
    def show_rows_returned(self, msg):
        self.set_status('{}'.format(msg))

    @QtCore.pyqtSlot(dict)
    def refresh_summary(self, totals):
        self.summary.setRowCount(len(totals))
        for i, (key, val) in enumerate(totals.items()):
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

        tbl.setRowCount(0) #len(self.model.totals))
        tbl.setColumnCount(2)
        tbl.setAlternatingRowColors(True)
        tbl.setHorizontalHeaderLabels(['Measure', 'Value'])
        tbl.resizeColumnsToContents()
        return tbl


class QueryDesigner(QtGui.QWidget):
    """Populate a layout with filter controls"""

    add_criteria_signal = QtCore.pyqtSignal(Criteria)
    error_signal = QtCore.pyqtSignal()
    export_signal = QtCore.pyqtSignal()
    pull_signal = QtCore.pyqtSignal()
    reset_signal = QtCore.pyqtSignal()
    stop_export_signal = QtCore.pyqtSignal()

    def __init__(self, filters: List[Filter], max_display_rows: int
                , max_export_rows: int) -> None:
        super(QueryDesigner, self).__init__()

        self._current_row = 0  # type: int
        self._filters = filters  # type List[Filter]
        self._max_display_rows = max_display_rows
        self._max_export_rows = max_export_rows

        self.layout = QtGui.QGridLayout()
        self.setLayout(self.layout)
        self.query_controls = {}  # type: Dict[str, Control]
        self.create_controls()

    @log_error
    def add_row(self, field_name: str, field_type: str, operator: str) -> None:
        lbl = QtGui.QLabel('{n} {o}'.format(n=field_name, o=operator))
        txt = QtGui.QLineEdit()
        ctrl_name = '{n}_{t}_{o}'.format(n=field_name, t=field_type, o=operator)
        self.query_controls[ctrl_name] = Control(handle=txt, type=field_type)
        self.layout.addWidget(lbl, self._current_row, 0, 1, 1)
        self.layout.addWidget(txt, self._current_row, 1, 1, 1)

        cmd = lambda v=txt.text(), n=field_name, t=field_type, o=operator, func=self.add_criteria:\
            func(value=v, field_name=n, field_type=t, operator=o)
        txt.textChanged.connect(cmd)

        self._current_row += 1

    def add_criteria(self, value: str, field_name: str, field_type: str, operator: str) -> None:
        criteria = Criteria(
            field_name=field_name
            , value=value
            , field_type=field_type
            , operator=operator
        )
        self.add_criteria_signal.emit(criteria)

    @log_error
    def create_controls(self) -> None:
        for f in self._filters[:10]:  # cap at 10 maximum filter input boxes
            self.add_row(field_name=f.field, field_type=f.type, operator=f.operator)

        self.btn_reset_query = QtGui.QPushButton('&Reset Query')
        self.btn_reset_query.clicked.connect(self.reset)
        self.layout.addWidget(self.btn_reset_query, self._current_row, 0, 1, 1)

        max_display_rows = '{:,}'.format(self._max_display_rows)
        pull_btn_txt = '&Show Top {} rows'.format(max_display_rows)
        self.btn_pull = QtGui.QPushButton(pull_btn_txt)
        self.btn_pull.clicked.connect(self.pull_signal.emit)
        self.layout.addWidget(self.btn_pull, self._current_row, 1, 1, 1)
        self._current_row += 1

        self.btn_stop_export = QtGui.QPushButton("Stop Export")
        self.btn_stop_export.clicked.connect(self.stop_export_signal.emit)
        self.layout.addWidget(self.btn_stop_export, self._current_row, 0, 1, 1)

        max_export_rows = '{:,}'.format(self._max_export_rows)
        export_btn_txt = 'E&xport Top {} rows'.format(max_export_rows)
        self.btn_export = QtGui.QPushButton(export_btn_txt)
        self.btn_export.clicked.connect(self.export_signal.emit)
        self.layout.addWidget(self.btn_export, self._current_row, 1, 1, 1)

    def reset(self):
        for key, val in self.query_controls.items():
            val.handle.setText('')
        self.reset_signal.emit()


class MainView(QtGui.QDialog):

    exit_signal = QtCore.pyqtSignal()

    def __init__(self, parent=None):
        super(MainView, self).__init__(parent
            , QtCore.Qt.WindowMinimizeButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint)

        self.config_popup = None
        self.datasheet_controls = []
        self.query_designer_visibility = True

        self.app_config = SimpleJsonConfig(os.path.join('config', 'app.json'))
        app_name = self.app_config.get_or_set_variable('app_name', 'TestApp')
        datasheets = self.app_config.get_or_set_variable(
            'datasheets', {
                'SalesHistory1': os.path.join('config', 'customer_config.json')
                , 'SalesHistory2': os.path.join('config', 'customer_config2.json')
            }
        )

        self.setWindowTitle(app_name)

        tabs = QtGui.QTabWidget()
        for name, config_path in sorted(datasheets.items()):
            config = query_manager_config(config_path)
            ds = DatasheetView(config=config)
            self.datasheet_controls.append(ds)
            self.exit_signal.connect(ds.exit_signal.emit)
            tabs.addTab(ds, name)

        mainLayout = QtGui.QVBoxLayout()
        menubar = QtGui.QMenuBar()
        mainLayout.addWidget(menubar)
        filemenu = menubar.addMenu('&File')
        filemenu.addAction('Open Output Folder', self.open_output_folder)
        settings_menu = filemenu.addMenu("Settings")
        settings_menu.addAction(
            'App'
            , lambda cfg_path=os.path.join('config', 'app.json')
            , func=self.open_settings: func(cfg_path)
        )
        for key, val in sorted(datasheets.items()):
            settings_menu.addAction(
                key
                , lambda cfg_path=val
                , func=self.open_settings: func(cfg_path)
            )
        filemenu = menubar.addMenu('&View')
        filemenu.addAction('Toggle Query Designer', self.toggle_query_designer)

        mainLayout.addWidget(tabs)
        self.setLayout(mainLayout)

    def open_output_folder(self):
        folder = os.path.join(rootdir(), 'output')
        if not os.path.exists(folder) or not os.path.isdir(folder):
            os.mkdir(folder)
        os.startfile(folder)

    def open_settings(self, config_path) -> None:
        self.config_popup = ConfigPopup(config_path)
        # self.config_popup.setGeometry(QtCore.QRect(100, 100, 400, 200))
        self.config_popup.show()

    def toggle_query_designer(self):
        if self.query_designer_visibility:
            for ds in self.datasheet_controls:
                ds.layout.setColumnStretch(0, 0)
                ds.query_designer.hide()
                self.query_designer_visibility = False
        else:
            for ds in self.datasheet_controls:
                ds.layout.setColumnStretch(0, 1)
                ds.query_designer.show()
                self.query_designer_visibility = True


class ConfigPopup(QtGui.QWidget):

    def __init__(self, config_path) -> None:
        super(ConfigPopup, self).__init__()
        self.setWindowTitle("Config")
        self.layout = QtGui.QVBoxLayout()
        self.setLayout(self.layout)
        self.layout.addWidget(QtGui.QLabel("file path: {}".format(config_path)))
        config = SimpleJsonConfig(json_path=config_path)
        for key, val in config.all_variables.items():
            self.layout.addWidget(QtGui.QLabel("{k}: {v}".format(k=key, v=val)))



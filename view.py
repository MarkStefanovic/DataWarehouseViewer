"""This module displays the data provided by the query manager"""

from collections import namedtuple, OrderedDict
from functools import partial
import os
from subprocess import Popen
import time
from typing import Dict, List

from PyQt4 import QtCore, QtGui
import xlwt

from checkbox_delegate import CheckBoxDelegate
from config import cfg
from foreign_key_delegate import ForeignKeyDelegate
from logger import log_error
from model import AbstractModel
from schema import Filter, Table, FieldType
from utilities import delete_old_outputs, rootdir, timestr


class Control:
    def __init__(self, *, handle, display_name: str, control_type) -> None:
        # TODO make type to represent pyqt controls
        self.handle = handle
        self.display_name = display_name
        self.control_type = control_type


class DatasheetView(QtGui.QWidget):
    """
    This class takes a model as an input and creates an editable datasheet therefrom.
    """

    def __init__(self, table: Table, parent=None):
        super(DatasheetView, self).__init__()
        self.setWindowFlags = (
            QtCore.Qt.WindowMinimizeButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint
        )
        self.model = AbstractModel(table=table)
        self.table = QtGui.QTableView()
        self.table.setSortingEnabled(True)
        self.query_controls = {}
        self.menu = QtGui.QMenu(self)

        self.table.setModel(self.model)

        self.hide_pk()

    #   CREATE WIDGETS
        self.top_button_box = QtGui.QDialogButtonBox()
        self.btn_reset = QtGui.QPushButton('&Reset Filters')
        # self.txt_search = QtGui.QLineEdit()
        self.lbl_search = QtGui.QLabel('Quick Search:')
        self.add_foreign_key_comboboxes()
        self.add_boolean_checkboxes()

        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table.setShowGrid(False)
        self.table.resizeColumnsToContents()
        # self.table.horizontalHeader().setResizeMode(QtGui.QHeaderView.Stretch)
        # self.table.horizontalHeader().setStretchLastSection(True)

    #   LAYOUT
        self.layout = QtGui.QGridLayout()
        self.setLayout(self.layout)
        self.layout.setColumnStretch(0, 1)  # rows designer
        self.layout.setColumnStretch(1, 6)  # ds and summary

        self.query_designer = QueryDesigner(filters=self.model.query_manager.filters)
        self.layout.addWidget(self.query_designer, 0, 0, 1, 1, QtCore.Qt.AlignTop)

        ds_layout = QtGui.QGridLayout()
        ds_layout.setColumnStretch(2, 4)
        ds_layout.addWidget(self.btn_reset, 0, 0, 1, 1)
        # ds_layout.addWidget(self.lbl_search, 0, 1, 1, 1)
        # ds_layout.addWidget(self.txt_search, 0, 2, 1, 1)
        ds_layout.addWidget(self.table, 1, 0, 1, 3)
        self.layout.addLayout(ds_layout, 0, 1, 1, 1, QtCore.Qt.AlignTop)


        bottom_bar = QtGui.QGridLayout()
        self.statusbar = QtGui.QStatusBar()
        self.statusbar.showMessage("")
        bottom_bar.addWidget(self.statusbar, 0, 0)
        self.btn_save = QtGui.QPushButton("Save")
        self.btn_undo = QtGui.QPushButton("Undo")
        bottom_bar.setColumnStretch(0, 10)
        bottom_bar.setColumnStretch(1, 1)
        bottom_bar.setColumnStretch(2, 1)
        if self.model.query_manager.table.editable:
            bottom_bar.addWidget(self.btn_undo, 0, 1)
            bottom_bar.addWidget(self.btn_save, 0, 2)
        self.layout.addLayout(bottom_bar, 1, 0, 1, 2)

    #   CONNECT SIGNALS
        # Error Signals
        self.query_designer.error_signal.connect(self.outside_error)
        self.model.error_signal.connect(self.outside_error)
        self.model.query_manager.error_signal.connect(self.outside_error)
        self.model.query_manager.runner.signals.error.connect(self.outside_error)
        self.model.query_manager.exporter.signals.error.connect(self.outside_error)

        # self.txt_search.textChanged.connect(self.on_lineEdit_textChanged)
        self.btn_reset.clicked.connect(self.reset)
        self.btn_save.clicked.connect(self.save)
        self.model.layoutChanged.connect(self.table.resizeColumnsToContents)
        self.model.query_manager.exporter.signals.rows_exported.connect(self.show_rows_exported)
        self.model.query_manager.runner.signals.rows_returned_msg.connect(self.show_rows_returned)
        # self.model.layoutChanged.connect(self.open_comboboxes)
        # self.model.rows_fetched_signal.connect(self.open_comboboxes)
        self.query_designer.add_criteria_signal.connect(self.add_query_criteria)
        # self.query_designer.error_signal.connect(self.set_status)
        self.query_designer.export_signal.connect(self.export_results)
        self.query_designer.pull_signal.connect(self.pull)
        self.query_designer.reset_signal.connect(self.reset_query)
        self.query_designer.stop_export_signal.connect(
            self.model.query_manager.exporter.signals.exit.emit)
        self.btn_undo.clicked.connect(self.undo)

    def add_boolean_checkboxes(self):
        for i, fld in enumerate(self.model.query_manager.table.fields):
            if fld.dtype == FieldType.bool:
                self.table.setItemDelegateForColumn(i, CheckBoxDelegate(self.model))

    def add_foreign_key_comboboxes(self) -> None:
        if self.model.query_manager.table.editable:
            for key, val in self.model.foreign_keys.items():
                dim = self.model.query_manager.table.foreign_keys[key].dimension
                self.table.setItemDelegateForColumn(
                    key,
                    ForeignKeyDelegate(
                        model=self.model,
                        dimension=dim
                    )
                )

    def add_query_criteria(self, filter_ix, value) -> None:
        self.model.query_manager.add_criteria(filter_ix, value)

    def exit(self):
        self.stop_everything.emit()

    def export_results(self) -> None:
        if self.model.visible_data:
            self.model.query_manager.export(
                rows=self.model.visible_rows,
                header=self.model.visible_header
            )
        self.set_status(msg="No rows to export")

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

    def hide_pk(self):
        pk = self.model.query_manager.table.primary_key_index
        self.table.hideColumn(pk)

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
        # self.txt_search.setText('')
        self.model.reset()

    # @QtCore.pyqtSlot(str)
    # def on_lineEdit_textChanged(self, text):
    #     self.model.filter_like(self.txt_search.text())

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
        self.set_status("{}: Pulling".format(timestr()))
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
            val = self.model.visible_data[row_ix][col_ix]
        except IndexError:
            val = ""

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
        self.list.itemChanged.connect(
            partial(
                self.on_list_selection_changed
                , col_ix=col_ix
            )
        )

        menu.addSeparator()
        menu.addAction(
            "Apply Checkbox Filters"
            , partial(
                self.apply_filter_set
                , col=col_ix
            )
        )

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

        if self.model.query_manager.table.editable:
            menu.addSeparator()
            model_ix = self.model.index(row_ix, col_ix)
            menu.addAction(
                "Add row"
                , partial(
                    self.model.add_row
                    , ix=model_ix
                )
            )
            menu.addAction(
                "Remove row"
                , partial(
                    self.model.delete_row
                    , ix=model_ix
                )
            )

        menu.addSeparator()
        menu.addAction("Open in Excel", self.export_results)

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
        checked_items = [
            itm for itm in all_items
            if not itm in [show_all_item, none_item]
               and itm.checkState() == QtCore.Qt.Checked
        ]
        unchecked_items = [
            itm for itm in all_items
            if not itm in [show_all_item, none_item]
               and itm.checkState() == QtCore.Qt.Unchecked
        ]
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
            [
                itm.setCheckState(QtCore.Qt.Checked)
                for itm in all_items
                if itm != show_all_item
            ]
            none_item.setCheckState(QtCore.Qt.Unchecked)

        def show_none():
            [
                itm.setCheckState(QtCore.Qt.Unchecked)
                for itm in all_items
                if itm != none_item
            ]
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

        self.filter_set = set(
            str(itm.text())
            for itm in all_items
            if itm.checkState() == QtCore.Qt.Checked
               and itm not in [show_all_item, none_item]
        )

    def apply_filter_set(self, col: int):
        self.model.filter_set(col=col, values=self.filter_set)

    @QtCore.pyqtSlot()
    def open_comboboxes(self):
        if self.model.query_manager.table.editable:
            for key, val in self.model.foreign_keys.items():
                for row in range(self.model.rowCount()):
                    self.table.openPersistentEditor(self.model.index(row, key))

    @QtCore.pyqtSlot(str)
    def outside_error(self, msg):
        self.set_status(msg)

    def reset_query(self):
        # self.txt_search.setText('')
        self.model.query_manager.reset()
        self.model.full_reset()
        self.set_status("")

    def save(self):
        try:
            results = self.model.save()
            if results:
                self.set_status(
                    "{} rows added; {} rows deleted; {} rows updated"
                    .format(
                        results['rows_added']
                        , results['rows_deleted']
                        , results['rows_updated']
                    )
                )
            else:
                self.set_status('No changes to save')
        except Exception as e:
            self.set_status("Error saving changes: {}".format(e))

    @QtCore.pyqtSlot(str)
    def set_status(self, msg: str) -> None:
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

    def reset_status(self):
        self.statusbar.showMessage("")

    def undo(self):
        self.set_status('')
        self.model.undo()


class QueryDesigner(QtGui.QWidget):
    """Populate a layout with filter controls"""

    add_criteria_signal = QtCore.pyqtSignal(int, str)
    error_signal = QtCore.pyqtSignal()
    export_signal = QtCore.pyqtSignal()
    pull_signal = QtCore.pyqtSignal()
    reset_signal = QtCore.pyqtSignal()
    stop_export_signal = QtCore.pyqtSignal()

    def __init__(self, filters: List[Filter]) -> None:
        super(QueryDesigner, self).__init__()

        self._current_row = 0  # type: int
        self.filters = filters  # type List[Filter]

        self.layout = QtGui.QGridLayout()
        self.layout.setColumnMinimumWidth(1, 120)
        self.setLayout(self.layout)
        self.query_controls = {}
        self.create_controls()

    # @log_error
    def add_row(self, filter: Filter) -> None:
        lbl = QtGui.QLabel(filter.display_name)
        txt = QtGui.QLineEdit()
        self.query_controls[self._current_row] = txt

        self.layout.addWidget(lbl, self._current_row, 0, 1, 1)
        self.layout.addWidget(txt, self._current_row, 1, 1, 1)

        cmd = lambda v=txt.text(), n=self._current_row, func=self.add_criteria:\
            func(filter_ix=n, value=v)
        txt.textChanged.connect(cmd)

        self._current_row += 1

    def add_criteria(self, filter_ix: int, value: str) -> None:
        self.add_criteria_signal.emit(filter_ix, value)

    # @log_error
    def create_controls(self) -> None:
        for f in self.filters[:10]:  # cap at 10 maximum filter input boxes
            self.add_row(f)

        self.btn_reset_query = QtGui.QPushButton('&Reset Query')
        self.btn_reset_query.clicked.connect(self.reset)
        self.layout.addWidget(self.btn_reset_query, self._current_row, 0, 1, 1)

        pull_btn_txt = 'Show'
        self.btn_pull = QtGui.QPushButton(pull_btn_txt)
        self.btn_pull.clicked.connect(self.pull_signal.emit)
        self.layout.addWidget(self.btn_pull, self._current_row, 1, 1, 1)
        self._current_row += 1

        self.btn_stop_export = QtGui.QPushButton("Stop Export")
        self.btn_stop_export.clicked.connect(self.stop_export_signal.emit)
        self.layout.addWidget(self.btn_stop_export, self._current_row, 0, 1, 1)

        export_btn_txt = 'Export'
        self.btn_export = QtGui.QPushButton(export_btn_txt)
        self.btn_export.clicked.connect(self.export_signal.emit)
        self.layout.addWidget(self.btn_export, self._current_row, 1, 1, 1)

    def reset(self):
        for val in self.query_controls.values():
            val.setText('')
        self.reset_signal.emit()


class MainView(QtGui.QDialog):

    exit_signal = QtCore.pyqtSignal()

    def __init__(self, parent=None):
        super(MainView, self).__init__(
            parent
            , QtCore.Qt.WindowMinimizeButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint
        )

        self.config_popup = None
        self.datasheet_controls = []
        self.query_designer_visibility = True

        app_name = cfg.app.display_name

        self.setWindowTitle(app_name)

        tabs = QtGui.QTabWidget()
        for tbl in cfg.tables:
            ds = DatasheetView(table=tbl)
            self.datasheet_controls.append(ds)
            # self.exit_signal.connect(ds.exit_signal.emit)
            tabs.addTab(ds, tbl.display_name)

        mainLayout = QtGui.QVBoxLayout()
        menubar = QtGui.QMenuBar()
        mainLayout.addWidget(menubar)
        filemenu = menubar.addMenu('&File')
        filemenu.addAction('Open Output Folder', self.open_output_folder)
        # settings_menu = filemenu.addMenu("Settings")
        # settings_menu.addAction(
        #     'App'
        #     , lambda cfg_path=os.path.join('config', 'app.json')
        #     , func=self.open_settings: func(cfg_path)
        # )
        # for key, val in sorted(datasheets.items()):
        #     settings_menu.addAction(
        #         key
        #         , lambda cfg_path=val
        #         , func=self.open_settings: func(cfg_path)
        #     )
        filemenu = menubar.addMenu('&View')
        filemenu.addAction('Toggle Query Designer', self.toggle_query_designer)

        mainLayout.addWidget(tabs)
        self.setLayout(mainLayout)

    def open_output_folder(self):
        folder = os.path.join(rootdir(), 'output')
        if not os.path.exists(folder) or not os.path.isdir(folder):
            os.mkdir(folder)
        os.startfile(folder)

    # def open_settings(self, config_path) -> None:
    #     self.config_popup = ConfigPopup(config_path)
    #     # self.config_popup.setGeometry(QtCore.QRect(100, 100, 400, 200))
    #     self.config_popup.show()

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


# class ConfigPopup(QtGui.QWidget):
#
#     def __init__(self, config_path) -> None:
#         super(ConfigPopup, self).__init__()
#         self.setWindowTitle("Config")
#         self.layout = QtGui.QVBoxLayout()
#         self.setLayout(self.layout)
#         self.layout.addWidget(QtGui.QLabel("file path: {}".format(config_path)))
#         config = SimpleJsonConfig(json_path=config_path)
#         for key, val in config.all_variables.items():
#             self.layout.addWidget(QtGui.QLabel("{k}: {v}".format(k=key, v=val)))

# if __name__ == "__main__":
    # import doctest
    # doctest.testmod()

    # import sys
    # app = QtGui.QApplication(sys.argv)
    # tbl = cfg.tables[0]
    # d = DatasheetView(tbl)
    # d.show()
    # app.exec_()
    # sys.exit(0)
    # print(q.table.filters)
    # print(q.foreign_keys)


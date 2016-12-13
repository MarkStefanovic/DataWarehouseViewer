"""This module displays the data provided by the query manager"""
import logging
import os
from collections import namedtuple, OrderedDict
from functools import partial

from PyQt4 import QtCore, QtGui
from PyQt4.QtGui import QComboBox
from PyQt4.QtGui import QLineEdit
from sortedcollections import ValueSortedDict
from typing import Dict, List

from delegates import (
    CellEditorDelegate,
    CheckBoxDelegate,
    ForeignKeyDelegate,
    SpinBoxDelegate,
    DateDelegate,
    RichTextColumnDelegate,
    PushButtonDelegate
)
from model import AbstractModel
from star_schema.constellation import (
    Filter,
    View,
    convert_value,
    DisplayPackage,
    Constellation)
from star_schema.custom_types import (
    Operator,
    FieldFormat,
    ColumnIndex,
    PrimaryKeyValue
)
from utilities import (
    rootdir,
    timestr,
    timestamp
)

module_logger = logging.getLogger('app')


class Control:
    def __init__(self, *, handle, display_name: str, control_type) -> None:
        self.handle = handle
        self.display_name = display_name
        self.control_type = control_type


class DatasheetView(QtGui.QWidget):
    """
    This class takes a model as an input and creates an editable datasheet therefrom.
    """

    def __init__(self, config: DisplayPackage, parent=None):
        self.logger = module_logger.getChild('DatasheetView')
        super().__init__()

        self.setWindowFlags = (
            QtCore.Qt.WindowMinimizeButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint
        )
        self.config = config

        self.model = AbstractModel(config=config)
        self.table = QtGui.QTableView()
        self.table.setSortingEnabled(True)
        self.query_controls = {}
        self.menu = QtGui.QMenu(self)

        self.table.setModel(self.model)

        self.hide_pk()

        self.top_button_box = QtGui.QDialogButtonBox()
        self.lbl_search = QtGui.QLabel('Quick Search:')

        self.add_delegates()

        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table.setShowGrid(False)
        self.table.resizeColumnsToContents()

        # self.table.resizeRowsToContents()
        # self.table.setWordWrap(True)

        # self.table.horizontalHeader().setResizeMode(QtGui.QHeaderView.Stretch)

        # Bubble sort column order by their display index
        header = self.table.horizontalHeader()
        order = [ix for ix in config.field_order_map.values()]
        vis_hdr = [fld.display_name for fld in config.fields]
        n = len(order)
        for i in range(n):
            for j in range(0, n-i-1):
                if order[j] > order[j+1]:
                    header.swapSections(j, j+1)
                    order[j], order[j+1] = order[j+1], order[j]
                    vis_hdr[j], vis_hdr[j+1] = vis_hdr[j+1], vis_hdr[j]

        # Save the new visible header order for later when exporting
        sorted_fields = config.fields_by_display_index.values()
        self.visible_header = [
            vis_hdr[i]
            for i, fld in enumerate(sorted_fields)
            if fld.visible
        ]

    #   LAYOUT
        self.layout = QtGui.QGridLayout()
        self.setLayout(self.layout)
        self.layout.setColumnStretch(0, 1)  # rows designer
        self.layout.setColumnStretch(1, 6)  # ds and summary

        self.query_designer = QueryDesigner(filters=config.filters)
        self.layout.addWidget(self.query_designer, 0, 0, 1, 1, QtCore.Qt.AlignTop)

        ds_layout = QtGui.QGridLayout()
        ds_layout.setColumnStretch(2, 4)
        ds_layout.addWidget(self.table, 1, 0, 1, 3)
        self.layout.addLayout(ds_layout, 0, 1, 1, 1, QtCore.Qt.AlignTop)

        bottom_bar = QtGui.QGridLayout()
        self.statusbar = QtGui.QStatusBar()
        bottom_bar.addWidget(self.statusbar, 0, 0)
        self.btn_save = QtGui.QPushButton("Save")
        self.btn_undo = QtGui.QPushButton("Undo")
        bottom_bar.setColumnStretch(0, 10)
        bottom_bar.setColumnStretch(1, 1)
        bottom_bar.setColumnStretch(2, 1)
        if self.config.table.editable:
            bottom_bar.addWidget(self.btn_undo, 0, 1)
            bottom_bar.addWidget(self.btn_save, 0, 2)
        self.layout.addLayout(bottom_bar, 1, 0, 1, 2)

    #   SIGNALS
        self.btn_save.clicked.connect(self.save)
        self.btn_undo.clicked.connect(self.undo)
        self.model.error_signal.connect(self.outside_error)
        self.model.exporter.signals.error.connect(self.outside_error)
        self.model.exporter.signals.rows_exported.connect(
            self.show_rows_exported)
        self.model.filters_changed_signal.connect(self.table.scrollToTop)
        self.model.layoutChanged.connect(self.table.resizeColumnsToContents)
        self.model.query_runner.signals.error.connect(self.outside_error)
        self.model.query_runner.signals.rows_returned_msg.connect(
            self.show_rows_returned)
        self.query_designer.error_signal.connect(self.outside_error)
        self.query_designer.export_signal.connect(self.export_results)
        self.query_designer.pull_signal.connect(self.pull)
        self.query_designer.reset_signal.connect(self.reset_query)
        self.query_designer.stop_export_signal.connect(
            self.model.exporter.signals.exit.emit)

    def add_delegates(self):
        delegate_lkp = {
            FieldFormat.Accounting: partial(CellEditorDelegate, self.table),
            FieldFormat.Bool:       partial(CheckBoxDelegate, self.table),
            FieldFormat.Currency:   partial(CellEditorDelegate, self.table),
            FieldFormat.Date:       partial(DateDelegate, self.table),
            FieldFormat.DateTime:   partial(DateDelegate, self.table),
            FieldFormat.Float:      partial(CellEditorDelegate, self.table),
            FieldFormat.Int:        partial(SpinBoxDelegate, self.table),
            FieldFormat.Str:        partial(CellEditorDelegate, self.table),
            FieldFormat.Memo:       partial(RichTextColumnDelegate, self.table)
        }

        if not self.config.table.editable:
            return

        for ix in self.config.editable_field_indices:
            try:
                if ix in self.config.foreign_key_indices:
                    fk_lkp_fn = self.config.foreign_keys_by_original_index[ix]
                    delegate = ForeignKeyDelegate(parent=self.table,
                        foreign_keys=fk_lkp_fn)
                else:
                    fld = self.config.fields_by_original_index[ix]
                    delegate = delegate_lkp[fld.field_format]()
                self.table.setItemDelegateForColumn(ix, delegate)
            except Exception as e:
                self.logger.debug(
                    'add_delegates: Error adding delegate for column {}; '
                    'error: {}'.format(ix, e)
                )

        # lookup fields are not editable on the table they are displayed on
        for ix in self.config.lookup_field_indices:
            delegate = PushButtonDelegate(self.table)
            delegate.buttonDoubleClicked.connect(self.open_lookup_field_popup)
            self.table.setItemDelegateForColumn(ix, delegate)

    @QtCore.pyqtSlot(int, int, int)
    def open_lookup_field_popup(self, row: int, col: ColumnIndex) -> None:
        # Select the button so that it is visually apprent which button you
        # have open.
        ix = self.model.index(row, col)
        self.table.selectionModel().select(ix, QtGui.QItemSelectionModel.Select)

        # look up the display package to send to the popup window
        pkg = self.config.lookup_field_display_packages_by_original_index[col]
        primary_key = self.model.primary_key(self.model.visible_data[row])
        popup = LookupFieldPopup(parent=self.table, config=pkg,
            primary_key=primary_key)
        popup.show()

    def exit(self):
        self.stop_everything.emit()

    def export_results(self) -> None:
        if self.model.displayed_data:
            self.model.export_visible(visible_header=self.visible_header)
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

    def hide_pk(self) -> None:
        try:
            pk = self.config.table.primary_key_index
            # views don't include the primary key, so we don't try and hide the
            # primary key on views
            if pk >= 0:
                self.table.hideColumn(pk)

            for ix, fld in self.config.fields_by_original_index.items():
                if not fld.visible:
                    self.table.hideColumn(ix)
        except Exception as e:
            self.logger.debug('hide_pk: {}'.format(e))

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
            super().keyPressEvent(event)

    @QtCore.pyqtSlot()
    def rclick_menu_delete(self):
        ids = self.get_all_selected_ids()
        self.model.delete_records(ids)

    def reset(self):
        self.table.resizeColumnsToContents()
        self.model.reset()

    def copy(self):
        """Copy selected cells into copy-buffer"""
        selection = self.table.selectionModel()
        indexes = selection.selectedIndexes()

        rows = OrderedDict()
        for ix in indexes:
            row = ix.row()
            rows[row] = rows.get(row, [])
            rows[row].append(str(ix.data()) or '')

        # create header to append to clipboard data
        first_row = min(rows.keys())
        header = [
            self.model.header[ix.column()]
            for ix in indexes
            if ix.row() == first_row
        ]

        str_array = ''
        str_array += '\t'.join([hdr for hdr in header]) + '\n'
        for row in rows.keys():
            str_array += '\t'.join([col for col in rows[row]])
            str_array += '\n'
        QtGui.QApplication.clipboard().setText(str_array)

    def pull(self) -> None:
        self.set_status("{}: Pulling".format(timestr()))

        for filter_display_name, text_box in self.query_designer.filter_refs.items():
            val = self.query_designer.get_text(text_box)
            try:
                flt = Filter.find_by_display_name(filter_display_name)
                fld = flt.field
                cval = convert_value(field_type=fld.dtype, value=val)
                flt.value = cval
            except:
                self.set_status(
                    "Invalid input: {} is not a {}"
                    .format(val, fld.dtype)
                )
                return

        self.model.pull()

    def contextMenuEvent(self, event) -> None:
        """Implements right-clicking on cell."""
        if not self.table.underMouse():
            return

        rows = sorted(set(index.row() for index in self.table.selectedIndexes()))
        cols = sorted(set(index.column() for index in self.table.selectedIndexes()))

        row_ix = rows[0] if rows else 0
        col_ix = cols[0] if cols else 0

        # If the user's mouse is not over a row or column in the table,
        # don't show the popup menu.
        if self.model.visible_data:
            if col_ix not in cols:
                return
        menu = QtGui.QMenu(self)
        menu = self.make_cell_context_menu(menu, row_ix, col_ix)
        menu.exec_(self.mapToGlobal(event.pos()))

    def make_cell_context_menu(self, menu, row_ix, col_ix):
        """Create the menu displayed when right-clicking on a cell."""
        try:
            val = self.model.visible_data[row_ix][col_ix]
        except IndexError:
            val = ""

        self.col_like = QLineEdit()
        self.col_like.setPlaceholderText("Text like")
        txt_wac = QtGui.QWidgetAction(menu)
        txt_wac.setDefaultWidget(self.col_like)
        menu.addAction(txt_wac)
        self.col_like.textChanged.connect(partial(self.filter_col_like, col_ix))
        menu.addSeparator()

        def add_list(height: int=100):
            widget = QtGui.QListWidget()
            widget.setFixedHeight(height)
            lst_wac = QtGui.QWidgetAction(menu)
            lst_wac.setDefaultWidget(widget)
            menu.addAction(lst_wac)
            return widget

        def add_item(list_widget, text, check_state=QtCore.Qt.Checked):
            i = QtGui.QListWidgetItem('%s' % text)
            i.setFlags(i.flags() | QtCore.Qt.ItemIsUserCheckable)
            i.setCheckState(check_state)
            list_widget.addItem(i)

        self.filter_by_list = add_list()
        add_item(self.filter_by_list, "Show All")
        add_item(self.filter_by_list, "None", QtCore.Qt.Unchecked)
        for itm in self.model.distinct_values(col_ix):
            if itm != "None":
                add_item(self.filter_by_list, itm)
        self.filter_by_list.itemChanged.connect(
            partial(self.on_list_selection_changed, col_ix=col_ix))

        menu.addSeparator()
        menu.addAction(
            "Apply Checkbox Filters",
            partial(
                self.apply_filter_set,
                col=col_ix
            )
        )

        menu.addSeparator()
        menu.addAction(
            "Show Equal To",
            partial(
                self.model.filter_equality,
                col_ix=col_ix,
                val=val
            )
        )
        menu.addAction(
            "Show Greater Than or Equal To",
            partial(
                self.model.filter_greater_than,
                col_ix=col_ix,
                val=val
            )
        )
        menu.addAction(
            "Show Less Than or Equal To",
            partial(
                self.model.filter_less_than,
                col_ix=col_ix,
                val=val
            )
        )
        menu.addSeparator()
        menu.addAction(
            "Reset Filters",
            self.reset
        )

        if self.config.table.editable:
            menu.addSeparator()
            model_ix = self.model.index(row_ix, col_ix)
            menu.addAction(
                "Add row",
                partial(
                    self.model.add_row,
                    ix=model_ix
                )
            )
            menu.addAction(
                "Remove row",
                partial(
                    self.model.delete_row,
                    ix=model_ix
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
        all_items = [self.filter_by_list.item(i) for i in range(len(self.filter_by_list))]

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

        self.filter_by_list.blockSignals(True)
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
        self.filter_by_list.blockSignals(False)

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
        if self.config.table.editable:
            for key, val in self.model.foreign_keys.items():
                for row in range(self.model.rowCount()):
                    self.table.openPersistentEditor(self.model.index(row, key))

    @QtCore.pyqtSlot(str)
    def outside_error(self, msg):
        self.set_status(msg)

    def reset_query(self):
        for flt in self.config.display_base.filters:
            flt.value = ''
        self.model.full_reset()
        self.set_status("Query results reset")

    def save(self):
        try:
            results = self.model.save()
            if results:
                self.set_status(
                    "{} rows added; {} rows deleted; {} rows updated"
                    .format(
                        results['rows_added'],
                        results['rows_deleted'],
                        results['rows_updated']
                    )
                )
            else:
                self.set_status('No changes to save')
        except Exception as e:
            err_msg = "Error saving changes: {}".format(e)
            self.logger.error('save: {}'.format(err_msg))
            self.set_status(err_msg)

    @QtCore.pyqtSlot(str)
    def set_status(self, msg: str) -> None:
        self.statusbar.showMessage('{t}: {m}'.format(t=timestamp(), m=msg))

    def show_query_designer(self):
        self.query_designer = self.query_layout()
        self.layout.addLayout(self.query_designer, 0, 0, 1, 1, QtCore.Qt.AlignTop)
        self.layout.setColumnStretch(0, 1)

    @QtCore.pyqtSlot(int)
    def show_rows_exported(self, msg):
        self.set_status('Rows exported {}'.format(msg))

    @QtCore.pyqtSlot(str)
    def show_rows_returned(self, msg):
        self.set_status('{}'.format(msg))

    def reset_status(self):
        self.set_status('Query results reset')

    def undo(self):
        self.set_status('Changes undone')
        self.model.undo()


class LookupFieldPopup(QtGui.QDialog):
    logger = module_logger.getChild('LookupFieldPopup')
    error_signal = QtCore.pyqtSignal(str)
    current_window = None

    def __init__(self, parent, config: DisplayPackage, primary_key: PrimaryKeyValue) -> None:
        super().__init__(parent)
        self.config = config
        self.primary_key = primary_key

        # If there is a pre-existing window, close it.
        try:
            if LookupFieldPopup.current_window:
                LookupFieldPopup.current_window.close()
        except:
            pass

        LookupFieldPopup.current_window = self

        self.setWindowTitle(config.display_name)

        self.list = QtGui.QListView()
        self.save_button = QtGui.QPushButton('Save Changes')
        self.save_button.clicked.connect(self.save_changes)

        self.model = QtGui.QStandardItemModel()

        self.distal_dim_fks = ValueSortedDict(config.table.distal_foreign_keys)
        self.initial_checked = config.table.lookup_keys.get(primary_key, [])
        for key, val in self.distal_dim_fks.items():
            item = QtGui.QStandardItem(val)
            if key in self.initial_checked:
                check = QtCore.Qt.Checked
            else:
                check = QtCore.Qt.Unchecked
            item.setCheckState(check)
            item.setCheckable(True)
            self.model.appendRow(item)

        self.list.setModel(self.model)
        self.layout = QtGui.QVBoxLayout()
        self.setLayout(self.layout)
        self.layout.addWidget(self.list)
        self.layout.addWidget(self.save_button)

        # self.model.itemChanged.connect(self.checkbox_event)

    # @QtCore.pyqtSlot(QtGui.QStandardItem)
    # def checkbox_event(self, item: QtGui.QStandardItem):
    #     checkstates ={
    #         0: 'unchecked',
    #         1: 'tristate',
    #         2: 'checked'
    #     }
    #     print('checkstate:', checkstates[item.checkState()])
    #     print('text:', item.text())

    def save_changes(self):
        """Save changes to the database"""
        def find_lkp_pk(proximal_fk, distal_fk):
            return next(
                key for key, val in self.config.table.data.items()
                if val[0] == proximal_fk and val[1] == distal_fk
            )

        def diff_changes(current_checked):
            original = set(self.initial_checked)
            modified = set(current_checked)
            added = (modified - original)
            added_rows = [
                (-(ix + 1), self.primary_key, a)
                for ix, a in enumerate(added)
            ]
            deleted = (original - modified)
            deleted_rows = [
                (find_lkp_pk(self.primary_key, d), self.primary_key, d)
                for d in deleted
            ]
            return {
                'added':   added_rows,
                'deleted': deleted_rows,
                'updated': {}
            }
        current = []
        for i, key in enumerate(self.distal_dim_fks.keys()):
            if self.model.item(i).checkState() == 2:
                current.append(key)

        changes = diff_changes(current)
        self.config.table.save_changes(changes)
        # LookupFieldPopup.logger.debug('save_changes: {}'.format(changes))
        self.close()


class QueryDesigner(QtGui.QWidget):
    """Populate a layout with filter controls"""

    add_criteria_signal = QtCore.pyqtSignal(int, str)
    error_signal = QtCore.pyqtSignal()
    export_signal = QtCore.pyqtSignal()
    pull_signal = QtCore.pyqtSignal()
    reset_signal = QtCore.pyqtSignal()
    stop_export_signal = QtCore.pyqtSignal()

    def __init__(self, filters: List[Filter]) -> None:
        super().__init__()

        self.logger = module_logger.getChild('QueryDesigner')

        self._current_row = 0  # type: int
        self.filters = filters  # type List[Filter]

        self.layout = QtGui.QGridLayout()
        self.layout.setColumnMinimumWidth(1, 120)
        self.setLayout(self.layout)
        self.query_controls = {}
        self.filter_refs = {}
        # self.filter_ixs = {}
        self.create_controls()

    def add_row(self, filter: Filter) -> None:
        lbl = QtGui.QLabel(filter.display_name)
        if filter.operator in [Operator.bool_is, Operator.bool_is_not]:
            txt = QComboBox()
            txt.addItems(['', 'True', 'False'])
        else:
            txt = QLineEdit()
        self.filter_refs[filter.display_name] = txt
        self.query_controls[self._current_row] = txt

        self.layout.addWidget(lbl, self._current_row, 0, 1, 1)
        self.layout.addWidget(txt, self._current_row, 1, 1, 1)

        self._current_row += 1

    def get_text(self, txt):
        if isinstance(txt, QComboBox):
            return txt.currentText()
        return txt.text()

    def create_controls(self) -> None:
        for f in self.filters[:20]:  # cap at 20 maximum filter input boxes
            self.add_row(f)

        self.btn_reset_query = QtGui.QPushButton('&Reset Query')
        self.btn_reset_query.setAutoDefault(False)
        self.btn_reset_query.clicked.connect(self.reset)
        self.layout.addWidget(self.btn_reset_query, self._current_row, 0, 1, 1)

        pull_btn_txt = 'Show'
        self.btn_pull = QtGui.QPushButton(pull_btn_txt)
        self.btn_pull.setAutoDefault(True)
        self.btn_pull.clicked.connect(self.pull_signal.emit)
        self.layout.addWidget(self.btn_pull, self._current_row, 1, 1, 1)
        self._current_row += 1

        self.btn_stop_export = QtGui.QPushButton("Stop Export")
        self.btn_stop_export.setAutoDefault(False)
        self.btn_stop_export.clicked.connect(self.stop_export_signal.emit)
        self.layout.addWidget(self.btn_stop_export, self._current_row, 0, 1, 1)

        export_btn_txt = 'Export'
        self.btn_export = QtGui.QPushButton(export_btn_txt)
        self.btn_export.setAutoDefault(False)
        self.btn_export.clicked.connect(self.export_signal.emit)
        self.layout.addWidget(self.btn_export, self._current_row, 1, 1, 1)

    def reset(self):
        for txt in self.query_controls.values():
            if isinstance(txt, QComboBox):
                txt.setCurrentIndex(0)  # index 0 is an empty string
            else:
                txt.setText('')
        self.reset_signal.emit()


class MainView(QtGui.QDialog):

    exit_signal = QtCore.pyqtSignal()
    reload_tab_signal = QtCore.pyqtSignal(int)

    def __init__(self, constellation: Constellation, parent=None):
        super().__init__(
            parent
            , QtCore.Qt.WindowMinimizeButtonHint
            | QtCore.Qt.WindowMaximizeButtonHint
        )
        self.constellation = constellation
        self.logger = logging.getLogger('MainView')
        self.config_popup = None
        self.datasheet_controls = []
        self.query_designer_visibility = True
        self._current_tab_index = 0
        self.tab_filters_loaded = set()

        app_name = self.constellation.app.display_name

        self.setWindowTitle(app_name)

        self.tabs = QtGui.QTabWidget()
        Tab = namedtuple('Tab', 'table_ref ds_ref')
        self.tab_indices = {}  # type: Dict[int, tuple]
        visible_packages = (
            pkg for pkg in self.constellation.display_packages
            if pkg.visible
        )
        for i, pkg in enumerate(visible_packages):
            if pkg.visible:
                ds = DatasheetView(config=pkg)
                tbl = pkg.table
                self.datasheet_controls.append(ds)
                self.tab_indices[i] = Tab(table_ref=tbl, ds_ref=ds)
                self.tabs.addTab(ds, tbl.display_name)

        self.tabs.currentChanged.connect(self.load_tab)
        self.tabs_loaded = set()
        if self.tab_indices[0].table_ref.show_on_load:
            self.load_tab(0)

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

        self.reload_tab_signal.connect(self.reload_tab)

        mainLayout.addWidget(self.tabs)
        self.setLayout(mainLayout)

    def closeEvent(self, QCloseEvent):
        def changes():
            changed_tabs = []
            for ix, tab in self.tab_indices.items():
                if tab.ds_ref.model.pending_changes:
                    changed_tabs.append(ix)
            return changed_tabs

        pending_changes = changes()

        def save_changes():
            for ix in pending_changes:
                self.tab_indices[ix].ds_ref.model.save()

        if pending_changes:
            response = QtGui.QMessageBox.question(
                self,
                'Pending changes',
                "Save pending changes?",
                QtGui.QMessageBox.Yes,
                QtGui.QMessageBox.No
            )
            if response == QtGui.QMessageBox.Yes:
                save_changes()
                QCloseEvent.ignore()
            else:
                QCloseEvent.accept()
        QCloseEvent.accept()

    def apply_default_filters(self, tab_index: int) -> None:
        if tab_index not in self.tab_filters_loaded:
            tab = self.tab_indices[tab_index]
            ds = tab.ds_ref

            for filter_name, txt_box in ds.query_designer.filter_refs.items():
                try:
                    flt = Filter.find_by_display_name(filter_name)
                    val = flt.default_value
                    if not txt_box.text():
                        txt_box.setText(val)
                except Exception as e:
                    self.logger.debug(
                        'apply_default_filters: error applying default filter '
                        '{}; err:'.format(filter_name, e)
                    )

            self.tab_filters_loaded.add(tab_index)

    @QtCore.pyqtSlot(int)
    def load_tab(self, tab_index: int) -> None:
        """If the table is set to show on load, load it the first time the tab is clicked"""
        self._current_tab_index = tab_index
        self.apply_default_filters(tab_index)
        tab = self.tab_indices[tab_index]
        ds = tab.ds_ref
        ds.query_designer.btn_pull.setDefault(True)

        # views share filters with their fact table, so when if the fact has any
        # filters applied, load the text boxes in the query designer with those
        # values since they are already applied anyways.
        def standardize_value(val):
            return str(val or '').strip().lower()

        for flt in ds.query_designer.filters:
            ctl = ds.query_designer.filter_refs[flt.display_name]
            if isinstance(ctl, QLineEdit): # hasattr(ctl, 'setText'):
                ctl_val = standardize_value(ctl.text())
                flt_val = standardize_value(flt.value)
                if ctl_val != flt_val:
                    ctl.setText(str(flt.value or ''))
                    self.reload_tab_signal.emit(tab_index)
            elif isinstance(ctl, QComboBox):
                ctl_val = standardize_value(ctl.currentText())
                flt_val = standardize_value(flt.value)
                if ctl_val != flt_val:
                    index = ctl.findText(ctl.currentText(), QtCore.Qt.MatchFixedString)
                    if index >= 0:
                        ctl.setCurrentIndex(index)
                        self.reload_tab_signal.emit(tab_index)

        pre_load = tab.table_ref.show_on_load
        if pre_load and tab_index not in self.tabs_loaded:
            # load the tab
            base = ds.config.display_base
            ds.query_designer.pull_signal.emit()
            if not isinstance(base, View):
                # we want the view to reload everytime we open it
                self.tabs_loaded.add(tab_index)

    @QtCore.pyqtSlot(int)
    def reload_tab(self, tab_index: int) -> None:
        if tab_index in self.tabs_loaded:
            self.tabs_loaded.remove(tab_index)

    def open_output_folder(self) -> None:
        folder = os.path.join(rootdir(), 'output')
        if not os.path.exists(folder) or not os.path.isdir(folder):
            os.mkdir(folder)
        os.startfile(folder)

    # def open_settings(self, config_path) -> None:
    #     self.config_popup = ConfigPopup(config_path)
    #     # self.config_popup.setGeometry(QtCore.QRect(100, 100, 400, 200))
    #     self.config_popup.show()

    def toggle_query_designer(self) -> None:
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


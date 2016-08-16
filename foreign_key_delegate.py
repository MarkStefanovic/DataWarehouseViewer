from typing import Dict

from PyQt4 import QtCore, QtGui


class ForeignKeyDelegate(QtGui.QStyledItemDelegate):
    """ComboBox view inside of a Table. It only shows the ComboBox when it is
       being edited.
    """
    refresh_signal = QtCore.pyqtSignal()

    def __init__(self, model, foreign_keys: Dict[int, str]) -> None:
        super().__init__(model)
        self.model = model
        self.foreign_keys = foreign_keys

    def createEditor(self, parent, option, index):
        """Create the ComboBox editor view."""
        self.editor = QtGui.QComboBox(parent)
        for key, val in self.foreign_keys.items():
            self.editor.addItem(val, key)
        self.editor.installEventFilter(self)
        return self.editor

    def setEditorData(self, editor, index):
        """Set the ComboBox's current index."""
        ix = self.editor.findText(index.data(), QtCore.Qt.MatchFixedString)
        if ix >= 0:
            self.editor.setCurrentIndex(ix)


    def setModelData(self, editor, model, index):
        """Set the table's model's data when finished editing."""
        cbo_index = editor.currentIndex()
        item_index = self.editor.itemData(cbo_index)
        model.setData(index, item_index, QtCore.Qt.DisplayRole)
        # model.setData(index, self.foreign_keys[item_index], QtCore.Qt.UserRole)
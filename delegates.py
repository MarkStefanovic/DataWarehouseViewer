from typing import Dict

from PyQt4 import QtCore, QtGui


class ComboboxDelegate(QtGui.QStyledItemDelegate):
    """ComboBox view inside of a Table. It only shows the ComboBox when it is
       being edited.
    """

    def __init__(self, model, itemlist=None):
        super().__init__(model)
        self.itemlist = itemlist

    def createEditor(self, parent, option, index):
        """Create the ComboBox editor view."""
        editor = QtGui.QComboBox(parent)
        editor.addItems(self.itemlist)
        editor.setCurrentIndex(0)
        editor.installEventFilter(self)
        return editor

    def setEditorData(self, editor, index):
        """Set the ComboBox's current index."""
        value = index.data(QtCore.Qt.DisplayRole)
        i = editor.findText(value)
        if i == -1:
            i = 0
        editor.setCurrentIndex(i)

    def setModelData(self, editor, model, index):
        """Set the table's model's data when finished editing."""
        value = editor.currentText()
        model.setData(index, value)


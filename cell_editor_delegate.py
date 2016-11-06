from PyQt4 import QtCore, QtGui


class CellEditorDelegate(QtGui.QItemDelegate):

    def createEditor(self, parent, option, index):
        # if index.column() == 2:
        return super(CellEditorDelegate, self).createEditor(parent, option, index)
        # return None

    def setEditorData(self, editor, index):
        # if index.column() == 2:
            # Gets display text if edit data hasn't been set.
        text = index.data(QtCore.Qt.EditRole) or index.data(QtCore.Qt.DisplayRole)
        editor.setText(text)
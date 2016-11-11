from PyQt4 import QtCore, QtGui

from schema.config import cfg


class ForeignKeyDelegate(QtGui.QStyledItemDelegate):
    """ComboBox view inside of a Table. It only shows the ComboBox when it is
       being edited.
    """

    def __init__(self, model, dimension: str) -> None:
        super().__init__(model)
        self.model = model
        self.dimension = dimension

    def createEditor(self, parent, option, index):
        """Create the ComboBox editor view."""
        self.editor = QtGui.QComboBox(parent)
        vals_displayed = set()
        for key, val in cfg.foreign_keys(self.dimension).items():
            if val not in vals_displayed:
                self.editor.addItem(val, key)
                vals_displayed.add(val)
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

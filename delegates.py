from PyQt4 import QtCore, QtGui

from star_schema.config import cfg


class CellEditorDelegate(QtGui.QItemDelegate):

    def createEditor(self, parent, option, index):
        return super(CellEditorDelegate, self).createEditor(parent, option, index)

    def setEditorData(self, editor, index):
        text = index.data(QtCore.Qt.EditRole) or index.data(QtCore.Qt.DisplayRole)
        editor.setText(text)


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


def convert_to_bool(val):
    if not val:  # falsey
        return False
    elif 'true' in str(val).lower():
        return True
    elif 'false' in str(val).lower():
        return False
    elif str(val).isnumeric(): #isinstance(val, int):
        if int(val) == 0:
            return False
        return True
    return True # truthy


class CheckBoxDelegate(QtGui.QStyledItemDelegate):
    """A delegate that places a fully functioning QCheckBox in every
    cell of the column to which it's applied
    """
    def __init__(self, parent) -> None:
        super(CheckBoxDelegate, self).__init__()

    def createEditor(self, parent, option, index) -> None:
        return

    def paint(self, painter, option, index) -> None:
        """Paint the checkbox."""
        try:
            checked = convert_to_bool(index.data())
            check_box_style_option = QtGui.QStyleOptionButton()

            if QtCore.Qt.ItemIsEditable:
                check_box_style_option.state |= QtGui.QStyle.State_Enabled
            else:
                check_box_style_option.state |= QtGui.QStyle.State_ReadOnly

            if checked:
                check_box_style_option.state |= QtGui.QStyle.State_On
            else:
                check_box_style_option.state |= QtGui.QStyle.State_Off

            check_box_style_option.rect = self.getCheckBoxRect(option)

            check_box_style_option.state |= QtGui.QStyle.State_Enabled
            QtGui.QApplication.style().drawControl(QtGui.QStyle.CE_CheckBox, check_box_style_option, painter)
        except Exception as e:
            print('error printing checkbox delegate {} for index {} option {}'
                  .format(str(e), index, option))

    def editorEvent(self, event, model, option, index) -> bool:
        """Change the data in the model and the state of the checkbox
        if the user presses the left mousebutton or presses
        Key_Space or Key_Select and this cell is editable. Otherwise do nothing.
        """
        if not QtCore.Qt.ItemIsEditable:
            return False
        elif event.type() == QtCore.QEvent.MouseButtonPress:
            return False
        elif event.type() == QtCore.QEvent.MouseButtonRelease or event.type() == QtCore.QEvent.MouseButtonDblClick:
            if event.button() != QtCore.Qt.LeftButton or not self.getCheckBoxRect(option).contains(event.pos()):
                return False
            if event.type() == QtCore.QEvent.MouseButtonDblClick:
                return True
        elif event.type() == QtCore.QEvent.KeyPress:
            return False
        self.setModelData(None, model, index)
        return True

    def setModelData (self, editor, model, index) -> None:
        """Save the changes to the model."""
        new_value = not convert_to_bool(index.data())
        model.setData(index, new_value, QtCore.Qt.EditRole)

    def getCheckBoxRect(self, option):
        check_box_style_option = QtGui.QStyleOptionButton()
        check_box_rect = QtGui.QApplication.style().subElementRect(QtGui.QStyle.SE_CheckBoxIndicator, check_box_style_option, None)
        check_box_point = QtCore.QPoint(
            option.rect.x() + option.rect.width()/2 - check_box_rect.width()/2,
            option.rect.y() + option.rect.height()/2 - check_box_rect.height()/2
        )
        return QtCore.QRect(check_box_point, check_box_rect.size())

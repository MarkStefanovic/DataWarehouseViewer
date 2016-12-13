import logging
import platform

from PyQt4 import QtCore, QtGui
from datetime import date
from typing import Callable, Dict

from model import AbstractModel
from star_schema.constellation import convert_value
from star_schema.custom_types import FieldType

module_logger = logging.getLogger('app.' + __name__)


class CellEditorDelegate(QtGui.QStyledItemDelegate):

    def createEditor(self, parent: QtGui.QWidget, option, index):
        return super().createEditor(parent, option, index)

    def setEditorData(self, editor, index):
        text = index.data(QtCore.Qt.EditRole) or index.data(QtCore.Qt.DisplayRole)
        editor.setText(text)


class ForeignKeyDelegate(QtGui.QStyledItemDelegate):
    """ComboBox view inside of a Table. It only shows the ComboBox when it is
       being edited.
    """
    logger = module_logger.getChild('ForeignKeyDelegate')

    def __init__(self, *,
            parent: QtGui.QWidget,
            foreign_keys: Callable[[], Dict[int, str]]
        ) -> None:

        super().__init__(parent)
        self.foreign_keys = foreign_keys

    def createEditor(self, parent, option, index):
        """Create the ComboBox editor view."""
        self.editor = QtGui.QComboBox(parent)
        vals_displayed = set()
        fks = self.foreign_keys() or {}
        for key, val in fks.items():
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
        try:
            cbo_index = editor.currentIndex()
            item_index = self.editor.itemData(cbo_index)
            model.setData(index, item_index, QtCore.Qt.DisplayRole)
        except Exception as e:
            ForeignKeyDelegate.logger.debug(
                'setModelData: Unable to set model data; error {}'
                .format(str(e))
            )

        # model.setData(index, self.foreign_keys[item_index], QtCore.Qt.UserRole)


def convert_to_bool(val):
    if not val:
        return False
    elif 'true' in str(val).lower():
        return True
    elif 'false' in str(val).lower():
        return False
    elif str(val).isnumeric():
        if int(val) == 0:
            return False
        return True
    return True


class CheckBoxDelegate(QtGui.QStyledItemDelegate):
    """A delegate that places a fully functioning QCheckBox in every
    cell of the column to which it's applied
    """
    logger = module_logger.getChild('CheckBoxDelegate')

    def __init__(self, parent) -> None:
        super().__init__(parent)

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
            CheckBoxDelegate.logger.debug(
                'paint: error printing checkbox delegate {} for index {} '
                'option {}'.format(str(e), index, option)
            )

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

    def setModelData (self, editor: QtGui.QWidget, model: AbstractModel, index: QtCore.QModelIndex) -> None:
        """Save the changes to the model."""
        new_value = not convert_to_bool(index.data())
        model.setData(index, new_value, QtCore.Qt.EditRole)

    def getCheckBoxRect(self, option: QtGui.QStyleOptionViewItem):
        check_box_style_option = QtGui.QStyleOptionButton()
        check_box_rect = QtGui.QApplication.style().subElementRect(QtGui.QStyle.SE_CheckBoxIndicator, check_box_style_option, None)
        check_box_point = QtCore.QPoint(
            option.rect.x() + option.rect.width()/2 - check_box_rect.width()/2,
            option.rect.y() + option.rect.height()/2 - check_box_rect.height()/2
        )
        return QtCore.QRect(check_box_point, check_box_rect.size())


class SpinBoxDelegate(QtGui.QStyledItemDelegate):
    logger = module_logger.getChild('SpinBoxDelegate')

    def __init__(self, parent: QtGui.QWidget):
        super().__init__(parent)
        # self.list_widget = QtGui.QWidget()

    def createEditor(self, parent: QtGui.QWidget,
                           option: QtGui.QStyleOptionViewItem,
                           index: QtCore.QModelIndex) -> QtGui.QWidget:
        editor = QtGui.QSpinBox(parent)
        editor.setMinimum(-2147483646)
        editor.setMaximum(2147483647)
        return editor

    def setEditorData(self, editor: QtGui.QWidget,
                            index: QtCore.QModelIndex) -> None:
        value = index.data(QtCore.Qt.EditRole) or index.data(QtCore.Qt.DisplayRole)
        value = convert_value(field_type=FieldType.Int, value=value)
        editor.setValue(value)

    def setModelData(self, editor: QtGui.QWidget,
                           model: QtCore.QAbstractItemModel,
                           index: QtCore.QModelIndex) -> None:
        try:
            editor.interpretText()
            value = editor.value()
            model.setData(index, value, QtCore.Qt.EditRole)
        except Exception as e:
            SpinBoxDelegate.logger(
                'setModelData: Unable to set the model data for row {}, col {};'
                'error {}'.format(index.row(), index.column(), e)
            )

    def updateEditorGeometry(self, editor: QtGui.QWidget,
                                   option: QtGui.QStyleOptionViewItem,
                                   index: QtCore.QModelIndex) -> None:
        editor.setGeometry(option.rect)


class PushButtonDelegate(QtGui.QStyledItemDelegate):
    buttonClicked = QtCore.pyqtSignal(int, int)
    buttonDoubleClicked = QtCore.pyqtSignal(int, int)

    def __init__(self, parent: QtGui.QWidget):
        super().__init__(parent)

    def editorEvent(self, event, model, option, index):
        if event.type() == QtCore.QEvent.MouseButtonDblClick:
            self.buttonDoubleClicked.emit(index.row(), index.column())
            return True
        # elif event.type() == QtCore.QEvent.MouseButtonPress:
        #     # self.buttonClicked.emit(index.row(), index.column())
        #     return True
        else:  # return default action
            return super().editorEvent(event, model, option, index)


class DateDelegate(QtGui.QStyledItemDelegate):
    logger = module_logger.getChild('DateDelegate')

    def __init__(self, parent: QtGui.QWidget):
        super().__init__(parent)

    def createEditor(self, parent: QtGui.QWidget,
            option: QtGui.QStyleOptionViewItem,
            index: QtCore.QModelIndex) -> QtGui.QWidget:
        editor = QtGui.QDateEdit(parent)
        # now = QtCore.QDate.currentDate()
        editor.setDisplayFormat('yyyy-MM-dd')
        editor.setMinimumDate(date.min)  # (now)
        editor.setMaximumDate(date.max)
        editor.setCalendarPopup(True)
        return editor

    def setEditorData(self, editor: QtGui.QWidget,
            index: QtCore.QModelIndex) -> None:
        # data = index.data()
        # if not isinstance(data, QtCore.QPyNullVariant):
        #     editor.setDate(QtCore.QDate.fromString(data))

        value = index.data(QtCore.Qt.EditRole) or index.data(
            QtCore.Qt.DisplayRole)
        value = convert_value(field_type=FieldType.Date, value=value)
        editor.setDate(value)
        # editor.setDate(QtCore.QDate.fromString(value))

    def setModelData(self, editor: QtGui.QWidget,
            model: QtCore.QAbstractItemModel,
            index: QtCore.QModelIndex) -> None:
        value = editor.date().toString('yyyy-MM-dd')
        model.setData(index, value, QtCore.Qt.EditRole)


# MEMO DELEGATE
class RichTextLineEdit(QtGui.QTextEdit):
    (Bold, Italic, Underline, StrikeOut, Monospaced, Sans, Serif,
    NoSuperOrSubscript, Subscript, Superscript) = range(10)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.monofamily = "courier"
        self.sansfamily = "helvetica"
        self.seriffamily = "times"
        self.setLineWrapMode(QtGui.QTextEdit.NoWrap)
        self.setTabChangesFocus(True)
        self.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        fm = QtGui.QFontMetrics(self.font())
        h = int(fm.height() * (1.4 if platform.system() == "Windows" else 1.2))
        self.setMinimumHeight(h)
        self.setMaximumHeight(int(h * 1.2))
        self.setToolTip("Press <b>Ctrl+M</b> for the text effects "
                        "menu and <b>Ctrl+K</b> for the color menu")

    def toggleItalic(self):
        self.setFontItalic(not self.fontItalic())

    def toggleUnderline(self):
        self.setFontUnderline(not self.fontUnderline())

    def toggleBold(self):
        if self.fontWeight() > QtGui.QFont.Normal:
            self.setFontWeight(QtGui.QFont.Normal)
        else:
            self.setFontWeight(QtGui.QFont.Bold)

    def sizeHint(self):
        return QtCore.QSize(self.document().idealWidth() + 5,
            self.maximumHeight())

    def minimumSizeHint(self):
        fm = QtGui.QFontMetrics(self.font())
        return QtCore.QSize(fm.width("WWWW"), self.minimumHeight())

    def contextMenuEvent(self, event):
        self.textEffectMenu()

    def keyPressEvent(self, event):
        if event.modifiers() & QtCore.Qt.ControlModifier:
            handled = False
            if event.key() == QtCore.Qt.Key_B:
                self.toggleBold()
                handled = True
            elif event.key() == QtCore.Qt.Key_I:
                self.toggleItalic()
                handled = True
            elif event.key() == QtCore.Qt.Key_K:
                self.colorMenu()
                handled = True
            elif event.key() == QtCore.Qt.Key_M:
                self.textEffectMenu()
                handled = True
            elif event.key() == QtCore.Qt.Key_U:
                self.toggleUnderline()
                handled = True
            if handled:
                event.accept()
                return
        if event.key() in (QtCore.Qt.Key_Enter, QtCore.Qt.Key_Return):
            self.returnProcessed.emit()
            event.accept()
        else:
            QtGui.QTextEdit.keyPressEvent(self, event)

    def colorMenu(self):
        pixmap = QtGui.QPixmap(22, 22)
        menu = QtGui.QMenu("Color")
        for text, color in (
            ("&Black", QtCore.Qt.black), ("B&lue", QtCore.Qt.blue),
            ("Dark Bl&ue", QtCore.Qt.darkBlue), ("&Cyan", QtCore.Qt.cyan),
            ("Dar&k Cyan", QtCore.Qt.darkCyan), ("&Green", QtCore.Qt.green),
            ("Dark Gr&een", QtCore.Qt.darkGreen),
            ("M&agenta", QtCore.Qt.magenta),
            ("Dark Mage&nta", QtCore.Qt.darkMagenta),
            ("&Red", QtCore.Qt.red), ("&Dark Red", QtCore.Qt.darkRed)
        ):
            color = QtGui.QColor(color)
            pixmap.fill(color)
            action = menu.addAction(QtGui.QIcon(pixmap), text, self.setColor)
            action.setData(color)
        self.ensureCursorVisible()
        menu.exec_(self.viewport().mapToGlobal(
            self.cursorRect().center()))

    def setColor(self):
        action = self.sender()
        if action is not None and isinstance(action, QtGui.QAction):
            color = QtGui.QColor(action.data())
            if color.isValid():
                self.setTextColor(color)

    def textEffectMenu(self):
        format = self.currentCharFormat()
        menu = QtGui.QMenu("Text Effect")
        for text, shortcut, data, checked in (
            ("&Bold", "Ctrl+B", RichTextLineEdit.Bold,
                self.fontWeight() > QtGui.QFont.Normal),
            ("&Italic", "Ctrl+I", RichTextLineEdit.Italic, self.fontItalic()),
            ("Strike &out", None, RichTextLineEdit.StrikeOut, format.fontStrikeOut()),
            ("&Underline", "Ctrl+U", RichTextLineEdit.Underline, self.fontUnderline()),
            ("&Monospaced", None, RichTextLineEdit.Monospaced,
                format.fontFamily() == self.monofamily),
            ("&Serifed", None, RichTextLineEdit.Serif,
                format.fontFamily() == self.seriffamily),
            ("S&ans Serif", None, RichTextLineEdit.Sans,
                format.fontFamily() == self.sansfamily),
            ("&No super or subscript", None, RichTextLineEdit.NoSuperOrSubscript,
                format.verticalAlignment() == QtGui.QTextCharFormat.AlignNormal),
            ("Su&perscript", None, RichTextLineEdit.Superscript,
                format.verticalAlignment() == QtGui.QTextCharFormat.AlignSuperScript),
            ("Subs&cript", None, RichTextLineEdit.Subscript,
                format.verticalAlignment() == QtGui.QTextCharFormat.AlignSubScript)):
            action = menu.addAction(text, self.setTextEffect)
            if shortcut is not None:
                action.setShortcut(QtGui.QKeySequence(shortcut))
            action.setData(data)
            action.setCheckable(True)
            action.setChecked(checked)
        self.ensureCursorVisible()
        menu.exec_(self.viewport().mapToGlobal(self.cursorRect().center()))

    def setTextEffect(self):
        action = self.sender()
        if action is not None and isinstance(action, QtGui.QAction):
            what = int(action.data()[0]) #action.data().toInt()[0]
            if what == RichTextLineEdit.Bold:
                self.toggleBold()
                return
            if what == RichTextLineEdit.Italic:
                self.toggleItalic()
                return
            if what == RichTextLineEdit.Underline:
                self.toggleUnderline()
                return
            format = self.currentCharFormat()
            if what == RichTextLineEdit.Monospaced:
                format.setFontFamily(self.monofamily)
            elif what == RichTextLineEdit.Serif:
                format.setFontFamily(self.seriffamily)
            elif what == RichTextLineEdit.Sans:
                format.setFontFamily(self.sansfamily)
            if what == RichTextLineEdit.StrikeOut:
                format.setFontStrikeOut(not format.fontStrikeOut())
            if what == RichTextLineEdit.NoSuperOrSubscript:
                format.setVerticalAlignment(
                    QtGui.QTextCharFormat.AlignNormal)
            elif what == RichTextLineEdit.Superscript:
                format.setVerticalAlignment(
                    QtGui.QTextCharFormat.AlignSuperScript)
            elif what == RichTextLineEdit.Subscript:
                format.setVerticalAlignment(
                    QtGui.QTextCharFormat.AlignSubScript)
            self.mergeCurrentCharFormat(format)

    def toSimpleHtml(self):
        html = ""
        black = QtGui.QColor(QtCore.Qt.black)
        block = self.document().begin()
        while block.isValid():
            iterator = block.begin()
            while iterator != block.end():
                fragment = iterator.fragment()
                if fragment.isValid():
                    format = fragment.charFormat()
                    family = format.fontFamily()
                    color = format.foreground().color()
                    text = QtCore.Qt.escape(fragment.text())
                    if format.verticalAlignment() == QtGui.QTextCharFormat.AlignSubScript:
                        text = "<sub>{}</sub>".format(text)
                    elif format.verticalAlignment() == QtGui.QTextCharFormat.AlignSuperScript:
                        text = "<sup>{}</sup>".format(text)
                    if format.fontUnderline():
                        text = "<u>{}</u>".format(text)
                    if format.fontItalic():
                        text = "<i>{}</i>".format(text)
                    if format.fontWeight() > QtGui.QFont.Normal:
                        text = "<b>{}</b>".format(text)
                    if format.fontStrikeOut():
                        text = "<s>{}</s>".format(text)
                    if color != black: # or family:
                        attribs = ""
                        if color != black:
                            attribs += ' color="%s"' % color.name()
                        if family:
                            attribs += ' face="%s"' % family
                        text = "<font{}>{}</font>".format(attribs, text)
                    html += text
                iterator += 1
            block = block.next()
        return html


class RichTextColumnDelegate(QtGui.QStyledItemDelegate):
    def __init__(self, parent=None):
        super().__init__(parent)

    def paint(self, painter, option, index):
        if index.model().data(index, QtCore.Qt.DisplayRole):
            text = str(index.model().data(index, QtCore.Qt.DisplayRole)).strip()
        else:
            text = ''
        palette = QtGui.QApplication.palette()
        document = QtGui.QTextDocument()
        document.setDefaultFont(option.font)
        # if option.state & QtGui.QStyle.State_Selected:
        #     document.setHtml(
        #         "<font color={}>{}</font>"
        #         .format(palette.highlightedText().color().name(), text)
        #     )
        # else:
        #     document.setHtml(text)
        document.setHtml(
            "<font color={}>{}</font>"
            .format(palette.highlightedText().color().name(), text)
        )
        painter.save()
        if option.state & QtGui.QStyle.State_Selected:
            color = palette.highlight().color()
        # else:
        #     color = QtGui.QColor(index.model().data(index, QtCore.Qt.BackgroundColorRole))
            painter.fillRect(option.rect, color)
        painter.translate(option.rect.x(), option.rect.y())
        document.drawContents(painter)
        painter.restore()

    def sizeHint(self, option, index):
        # if index.model().data(index):
        #     text = str(index.model().data(index)).strip()
        # else:
        #     text = ''
        document = QtGui.QTextDocument()
        document.setDefaultFont(option.font)
        return QtCore.QSize(document.idealWidth() + 5, option.fontMetrics.height())

    def createEditor(self, parent, option, index):
        lineedit = RichTextLineEdit(parent)
        return lineedit

    def setEditorData(self, editor, index):
        if index.model().data(index, QtCore.Qt.DisplayRole):
            value = str(index.model().data(index, QtCore.Qt.DisplayRole)).strip()
        else:
            value = ""
        editor.setHtml(value)

    def setModelData(self, editor, model, index):
        model.setData(index, editor.toSimpleHtml())

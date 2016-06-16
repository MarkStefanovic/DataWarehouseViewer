from PyQt4 import QtCore

from logger import global_logger

class MessageQueue(QtCore.QObject):
    exit_signal = QtCore.pyqtSignal()
    error_signal = QtCore.pyqtSignal()
    rows_returned_signal = QtCore.pyqtSignal(int)

    def exit(self):
        self.exit_signal.emit()

    def errored(self, error_msg):
        global_logger.error(error_msg)
        self.error_signal.emit()

    def rows_returned(self, row_ct):
        self.rows_returned_signal.emit(row_ct)

global_message_queue = MessageQueue()

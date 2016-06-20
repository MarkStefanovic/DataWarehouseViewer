from PyQt4 import QtCore

from logger import global_logger

class MessageQueue(QtCore.QObject):
    exit_signal = QtCore.pyqtSignal()
    error_signal = QtCore.pyqtSignal()

    def exit(self):
        self.exit_signal.emit()

    def errored(self, error_msg):
        global_logger.error(error_msg)
        self.error_signal.emit()

global_message_queue = MessageQueue()

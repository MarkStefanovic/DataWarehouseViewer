from PyQt4 import QtCore

from logger import global_logger

class MessageQueue(QtCore.QObject):
    exit_signal = QtCore.pyqtSignal()
    error_signal = QtCore.pyqtSignal()
    errors = set()

    def exit(self):
        self.exit_signal.emit()

    def errored(self, error_msg):
        if len(self.errors) > 100:
            self.errors = set()
        if error_msg not in self.errors:
            global_logger.error(error_msg)
            self.error_signal.emit()
        self.errors.add(error_msg)

global_message_queue = MessageQueue()

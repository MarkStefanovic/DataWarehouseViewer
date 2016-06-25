import os
import sys

from PyQt4 import QtGui

from view import MainView
from logger import rotating_log, global_logger
from messenger import global_message_queue


if __name__ == '__main__':
    app = QtGui.QApplication(sys.argv)
    try:
        # app.setStyle('cleanlooks')
        app.setStyle("plastique")
        css_path = os.path.join('css', 'darkcity.css')
        with open(css_path, 'r') as fh:
            style_sheet = fh.read()
        app.setStyleSheet(style_sheet)

    #   set font (we set it here to more easily keep it consistent)
        font = QtGui.QFont("Arial", 11)
        app.setFont(font)

        icon_path = os.path.join('images', 'app.ico')
        icon = QtGui.QIcon(icon_path)
        app.setWindowIcon(icon)

        main_view = MainView()
        main_view.showMaximized()
        app.exec_()
        global_message_queue.exit()
        sys.exit(0)
    except SystemExit:
        print("Closing Window...")
        os._exit(0)  # cheap hack
    except Exception as e:
        global_logger.error(sys.exc_info()[1])
        global_logger.error(str(e))
        sys.exit(app.exec_())
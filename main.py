"""This app displays data from a database in a way that is easy to filter and export.
"""

import os
import sys

from PyQt4 import QtGui

from view import MainView
from logger import rotating_log


if __name__ == '__main__':
    app = QtGui.QApplication(sys.argv)
    main_view = MainView()
    main_logger = rotating_log('main')

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

        main_view.showMaximized()
        app.exec_()
        sys.exit(0)
    except SystemExit:
        os._exit(0)
    except Exception as e:
        main_logger.error("Error {}".format(e))
        sys.exit(app.exec_())
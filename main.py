"""This app displays data from a database in a way that is easy to filter and export.

FLOW OF INFORMATION
View (Input) -> Model -> Query Manager -> Exporter|Runner -> Data -> Query Manager -> Model -> View (Display):

No shortcuts are taken.
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
        print("Closing Window...")
        main_view.exit_signal.emit()
        os._exit(0)  # cheap hack
    except Exception as e:
        err_msg = "Error {}".format(e)
        main_logger.error(err_msg)
        sys.exit(app.exec_())
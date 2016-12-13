"""Entry point into the program"""
import logging
import os
import sys
from logging.config import dictConfig

from PyQt4 import QtGui

from logger import default_config
from star_schema.constellation import get_constellation
from view import MainView

logging_config = default_config()


if __name__ == '__main__':
    app = QtGui.QApplication(sys.argv)
    constellation = get_constellation(os.path.join('constellations', 'ireadgud.json'))
    main_view = MainView(constellation=constellation)
    dictConfig(logging_config)
    logger = logging.getLogger('app')

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
        logger.error("Error {}".format(e))
        sys.exit(app.exec_())

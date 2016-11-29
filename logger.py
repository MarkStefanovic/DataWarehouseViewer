import logging
import os

from utilities import rootdir

LOG_LEVEL = logging.DEBUG


def default_config():
    folder = os.path.join(rootdir(), 'logs')
    if not os.path.exists(folder) or not os.path.isdir(folder):
        os.mkdir(folder)

    fp = os.path.join(folder, 'rotating.log')

    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'}
        },
        'handlers': {
            'stream': {
                'class':        'logging.StreamHandler',
                'formatter':    'simple',
                'level':        logging.DEBUG
            },
            'file': {
                'class':        'logging.handlers.TimedRotatingFileHandler',
                'filename':     fp,
                'when':         'D',
                'interval':     1,
                'backupCount':  5,
                'formatter':    'simple',
                'level':        logging.ERROR
            },
        },
        'loggers': {
            'app': {
                'handlers': ['stream', 'file'],
                'level':    LOG_LEVEL
            }
        }
    }

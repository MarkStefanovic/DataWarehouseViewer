import logging
from logging.handlers import TimedRotatingFileHandler
import os


def rotating_log(error_level: str='error'):
    """Return a handle to a logger that messages can be sent to for storage."""

    error_levels = {
        'info': logging.INFO
        , 'debug': logging.DEBUG
        , 'error': logging.ERROR
    }

    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format)
    fp = os.path.join('logs', 'rotating.log')
    handler = TimedRotatingFileHandler(fp, when='m', interval=1, backupCount=1)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(error_levels.get(error_level, logging.ERROR))
    logger.addHandler(handler)
    return logger

global_logger = rotating_log('error')
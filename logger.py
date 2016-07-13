import functools
import logging
import os

from logging.handlers import TimedRotatingFileHandler
from utilities import rootdir


def rotating_log(name: str='main', error_level: str='error'):
    """Return a handle to a logger that messages can be sent to for storage."""

    error_levels = {
        'info': logging.INFO
        , 'debug': logging.DEBUG
        , 'error': logging.ERROR
    }

    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format)
    fp = os.path.join(rootdir(), 'logs', 'rotating.log')
    handler = TimedRotatingFileHandler(fp, when='D', interval=1, backupCount=1)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(error_levels.get(error_level, logging.ERROR))
    logger.addHandler(handler)
    return logger


def log_error(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not logging.getLogger('main').handlers:
            rotating_log()
        log_name = '{}.{}.{}'.format(
            'main'
            , func.__module__
            , func.__name__
        )
        current_logger = logging.getLogger(log_name)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            current_logger.exception(str(e))
            raise e
    return wrapper


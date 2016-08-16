"""
>>> @log_error
... def divtest():
...     debug_logger().info('test')
>>> divtest() # doctest:+ELLIPSIS
*etc* - debug - INFO - test
"""
import functools
import logging
import os
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
import sys
from typing import Callable

from utilities import rootdir

DEBUG = True


def rotating_log(name: str='main') -> logging.Logger:
    """Return a handle to a logger that messages can be sent to for storage."""
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fp = os.path.join(rootdir(), 'logs', 'rotating.log')
    file_handler = TimedRotatingFileHandler(
        filename=fp,
        when='D',
        interval=1,
        backupCount=5
    )
    ''' When Interval Options
        Value       Interval
        'S' 	    Seconds
        'M' 	    Minutes
        'H' 	    Hours
        'D' 	    Days
        'W0'-'W6' 	Weekday (0=Monday)
        'midnight' 	Roll over at midnight
        '''
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.setLevel(logging.ERROR)
    if DEBUG:
        stream_handler = StreamHandler(stream=sys.stderr)
        stream_handler.setLevel(logging.ERROR)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger


def debug_logger():
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    stream_handler = StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger('debug')
    logger.addHandler(stream_handler)
    logger.setLevel(logging.DEBUG)
    if not DEBUG:
        logger.propagate = False
    return logger


def log_error(func) -> Callable:
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


# if __name__ == '__main__':
#     import doctest
#     doctest.ELLIPSIS_MARKER = '*etc*'
#     doctest.testmod()
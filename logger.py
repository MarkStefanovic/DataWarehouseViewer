import functools
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
    handler = TimedRotatingFileHandler(fp, when='D', interval=1, backupCount=1)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger = logging.getLogger("main")
    logger.setLevel(error_levels.get(error_level, logging.ERROR))
    logger.addHandler(handler)
    return logger


# def log_exception(function):
#     """
#     A decorator that wraps the passed in function and logs
#     exceptions should one occur
#
#     Example usage:
#         @log_exception
#         def zero_divide():
#             1 / 0
#     """
#     # global_logger = rotating_log('error')
#
#     @functools.wraps(function)
#     def wrapper(*args, **kwargs):
#         nonlocal global_logger
#         try:
#             return function(*args, **kwargs)
#         except:
#             # log the exception
#             err = "There was an exception in  "
#             err += function.__name__
#             global_logger.exception(err)
#
#             # re-raise the exception
#             raise
#     return wrapper

global_logger = rotating_log('error')

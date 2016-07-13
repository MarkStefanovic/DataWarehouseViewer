import os
import re
import sys
import time
from sqlite3 import Cursor
from typing import Generator, Iterable, Sequence


def chunks(seq: Sequence, n: int) -> Generator:
    """Yield successive n-sized chunks from l.

    Examples:
    >>> list(chunks([1,2,3,4,5,6,7,8,9,10], 5))
    [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]
    """
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


def timestr() -> str:
    return time.strftime("%H:%M:%S")


def is_float(val) -> bool:
    """Can value be converted to a float?"""
    try:
        float(val)
        return True
    except ValueError:
        return False


def iterrows(cursor: Cursor, chunksize: int=1000) -> Generator:
    rows = 0  # type: int
    while True:
        results = cursor.fetchmany(chunksize)  # type: Iterable
        rows += chunksize
        if not results:
            break
        for result in results:
            yield result


def files_in_folder(folder: str, prefix: str=None) -> list:
    if prefix:
        return sorted([os.path.abspath(fp) for fp in os.listdir(folder) if fp.startswith(prefix)])
    return sorted([os.path.abspath(fp) for fp in os.listdir(folder)])


def rootdir() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.abspath(os.path.dirname(sys.executable))
    else:
        # return os.path.abspath(__file__)
        return os.path.abspath(os.path.dirname(__file__))

def valid_sql_field_name(field_name: str) -> bool:
    """Is the string a valid field name

    In order to be considered a valid field name the field must start with
    a letter and contain only alphanumeric characters thereafter.

    Example:
    >>> valid_sql_field_name('Test')
    True
    >>> valid_sql_field_name('!test@')
    False
    """
    if field_name.isalnum() and re.match(r"^\w", field_name):
        return True
    return False

if __name__ == "__main__":
    import doctest
    doctest.testmod()
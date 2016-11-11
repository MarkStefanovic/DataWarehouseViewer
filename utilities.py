"""The functions used in the module are used by multiple modules in the project"""

from functools import wraps
import os
import re
import sys
import time

from typing import Any, Generator, NamedTuple, Sequence


def cache(func):
    saved = {}

    @wraps(func)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in saved:
            saved[key] = func(*args, **kwargs)
        return saved[key]
    return memoizer


def chunks(seq: Sequence, n: int) -> Generator:
    """Yield successive n-sized chunks from l.

    Examples:
    >>> list(chunks([1,2,3,4,5,6,7,8,9,10], 5))
    [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]
    """
    for i in range(0, len(seq), n):
        yield seq[i:i+n]


def delete_old_outputs(path: str):
    """Delete old Excel exports from the output folder

    If a file is open it will trigger an OS Error,
    but we ignore it.  The file will get deleted later.
    """
    pattern = r'^tmp_.*\d{4}-\d{2}-\d{2}[.]\d{6}.xls$'
    for root, dirs, files in os.walk(path):
        for f in files:
            match = re.search(pattern, f)
            if match is not None:
                try:
                    os.remove(os.path.join(root, f))
                except:
                    pass

def timestamp() -> str:
    return time.strftime('%I:%M:%S %p')

def timestr() -> str:
    return time.strftime("%H:%M:%S")


def files_in_folder(folder: str, prefix: str=None) -> list:
    if prefix:
        return sorted([os.path.abspath(fp) for fp in os.listdir(folder) if fp.startswith(prefix)])
    return sorted([os.path.abspath(fp) for fp in os.listdir(folder)])


class DictToClassRepr:
    """Return a nested class representation of a dictionary.

    Examples:
        >>> d = {'a': 1, 'b': [1, 2, 3], 'c': {'d': 4, 'e': 5}}
        >>> o = DictToClassRepr(d)
        >>> o.c.e
        5

        >>> d = {'a': 1, 'b': [1, 2, 3], 'c': {'d': 4, 'e': 5}}
        >>> o = DictToClassRepr(d)
        >>> o.b
        [1, 2, 3]
    """
    def __init__(self, dictionary):
        for key, val in dictionary.items():
            if isinstance(val, (list, tuple)):
                setattr(
                    self
                    , key
                    , [DictToClassRepr(x)
                        if isinstance(x, dict)
                        else x for x in val]
                )
            else:
                setattr(
                    self
                    , key
                    , DictToClassRepr(val) if isinstance(val, dict) else val
                )


def rootdir() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.abspath(os.path.dirname(sys.executable))
    else:
        return os.path.abspath(os.path.dirname(__file__))



# if __name__ == "__main__":
#     import doctest
#     doctest.testmod()

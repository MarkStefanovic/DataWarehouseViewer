from functools import wraps
import os
import re
import sys
import time
import sqlite3
from typing import Any, Dict, Generator, Iterable, List, NamedTuple, Sequence


SqliteField = NamedTuple('SqliteField', [
    ('ix', int)
    , ('name', str)
    , ('data_type', str)
    , ('nullable', bool)
    , ('default_value', Any)
    , ('primary_key', bool)
])


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
    pattern = r'^temp_\d{4}-\d{2}-\d{2}[.]\d{6}.xls$'
    for root, dirs, files in os.walk(path):
        for f in files:
            match = re.search(pattern, f)
            if match is not None:
                try:
                    os.remove(os.path.join(root, f))
                except:
                    pass


def timestr() -> str:
    return time.strftime("%H:%M:%S")


def inspect_table(db: str, table: str) -> List[SqliteField]:
    with SQLiteConnection(db) as con:
        cursor = con.cursor()
        cursor.execute("PRAGMA table_info('{}')".format(table))
        return [
            SqliteField(
                ix=row[0]
                , name=row[1]
                , data_type=row[2]
                , nullable= True if row[3] == 1 else False
                , default_value=row[4]
                , primary_key=True if row[5] == 1 else False
            )
            for row in cursor.fetchall()
        ]


def is_float(val) -> bool:
    """Can value be converted to a float?"""
    try:
        float(val)
        return True
    except ValueError:
        return False


def iterrows(cursor: sqlite3.Cursor, chunksize: int=1000) -> Generator:
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


class immutable_property:
    """A method decorator to lazily evaluate a property value.

    This decorator should only be used to represent immutable data, as
    it replaces the property itself with its value for efficiency in
    future calls.  It's usually 4-7X faster to access the property, which
    is useful if the property is called millions of times.

    Example:
    >>> class Test:
    ...     def __init__(self, val):
    ...         self.val = val
    ...
    ...     def calc_return_value(self):
    ...         print('generating', self.val)
    ...
    ...     @immutable_property
    ...     def a(self):
    ...         self.calc_return_value()
    ...         return self.val

    >>> a_test = Test(2)
    >>> print('a_test.a:', a_test.a)
    generating 2
    a_test.a: 2
    >>> b_test = Test(3)
    >>> print('b_test:', b_test.a)
    generating 3
    b_test: 3
    >>> print('a_test.a:', a_test.a)
    a_test.a: 2
    >>> print('b_test:', b_test.a)
    b_test: 3
    """

    def __init__(self, prop):
        self.prop = prop
        self.prop_name = prop.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return None
        value = self.prop(obj)
        setattr(obj, self.prop_name, value)
        return value


class DictToClassRepr:
    """Return a nested class representation of a dictionary.

    Examples:
        >>> d = {'a': 1, 'b': [1, 2, 3], 'c': {'d': 4, 'e': 5}}
        >>> o = ObjRepr(d)
        >>> o.c.e
        5

        >>> d = {'a': 1, 'b': [1, 2, 3], 'c': {'d': 4, 'e': 5}}
        >>> o = ObjRepr(d)
        >>> o.b
        [1, 2, 3]
    """
    def __init__(self, dictionary):
        for key, val in dictionary.items():
            if isinstance(val, (list, tuple)):
                setattr(
                    self
                    , key
                    , [ObjRepr(x) if isinstance(x, dict) else x for x in val]
                )
            else:
                setattr(
                    self
                    , key
                    , ObjRepr(val) if isinstance(val, dict) else val
                )


def rootdir() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.abspath(os.path.dirname(sys.executable))
    else:
        return os.path.abspath(os.path.dirname(__file__))


class SQLiteConnection:
    """Context manager that auto-commits and closes connection on exit."""

    def __init__(self, db_name, read_only=False):
        """Constructor"""
        self.db_name = db_name
        self.read_only = read_only

    def __enter__(self):
        """
        Open the database connection
        """
        fp = os.path.abspath(self.db_name)
        if self.read_only:
            self.conn = sqlite3.connect('file:/{}?mode=ro'.format(fp), uri=True)
        else:
            self.conn = sqlite3.connect(fp)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the connection
        """
        if not self.read_only:
            self.conn.commit()
        self.conn.close()


def sqlite_pull(db_name, sql) -> List:
    with SQLiteConnection(db_name=db_name, read_only=True) as con:
        cursor = con.cursor()
        cursor.execute(sql)
        return cursor.fetchall()


def sqlite_delete_row(db_name, table, id_fieldname, id_value) -> None:
    with SQLiteConnection(db_name=db_name, read_only=False) as con:
        con.execute(
            "DELETE FROM {t} WHERE {n} = {v}"
            .format(t=table, n=id_fieldname, v=id_value)
        )


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
    if re.match(r"^[a-zA-Z]+_?[a-zA-Z]*$", field_name):
        return True
    return False

if __name__ == "__main__":
    import doctest
    doctest.testmod()
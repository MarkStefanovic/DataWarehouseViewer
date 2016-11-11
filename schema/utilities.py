"""The functions used in the module are used by multiple modules in the project"""

from functools import wraps
import os
import re
from reprlib import recursive_repr
import sys
import time

from typing import Any, Generator, NamedTuple, Sequence

SqliteField = NamedTuple('SqliteField', [
    ('ix', int)
    , ('name', str)
    , ('data_type', str)
    , ('nullable', bool)
    , ('default_value', Any)
    , ('primary_key', bool)
])


def autorepr(cls):
    """Class decorator that automatically adds __repr__ and __str__ methods.

    Example:
        >>> @autorepr
        ... class Test:
        ...     def __init__(self, a, b):
        ...         self.a = a
        ...         self.b = b

        >>> t = Test('hello', 'world')
        >>> t.b = t
        >>> print(t)
        Test
            a = hello
            b = ...
    """
    @recursive_repr()
    def __repr__(self):
        attrs = ", ".join(
            "{}={}".format(k, v) for k, v in self.__dict__.items())
        return "{}({})".format(self.__class__.__name__, attrs)

    @recursive_repr()
    def __str__(self):
        attrs = "\n".join(
            "    {} = {}".format(k, v) for k, v in self.__dict__.items())
        return "{}\n{}".format(self.__class__.__name__, attrs)

    cls.__repr__ = __repr__
    cls.__str__ = __str__
    return cls


def timestamp() -> str:
    return time.strftime('%I:%M:%S %p')


def timestr() -> str:
    return time.strftime("%H:%M:%S")


class static_property:
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
    ...     @static_property
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

# if __name__ == "__main__":
#     import doctest
#     doctest.testmod()

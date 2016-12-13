"""The functions used in the module are used by multiple modules in the project"""
import re
import time
from reprlib import recursive_repr

import sqlparse
from sqlalchemy.dialects import sqlite


def autorepr(*args, **kwargs):
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
        Test:
            a = hello
            b = ...
    """
    str_attrs = kwargs.get('str_attrs', [])
    repr_attrs = kwargs.get('repr_attrs', [])

    def include_str_attr(attr):
        if str_attrs:
            return attr in str_attrs
        else:
            return True

    def include_repr_attr(attr):
        if repr_attrs:
            return attr in repr_attrs
        else:
            return True

    def wrapper(cls):
        # print('class {}; str_attrs: {}; repr_attrs: {}'
        #     .format(cls.__name__, str_attrs, repr_attrs))
        @recursive_repr()
        def __repr__(self):
            attrs = ", ".join(
                "{}={}".format(k, v)
                    for k, v in self.__dict__.items()
                    if include_repr_attr(k)
            )
            return "{}({})".format(self.__class__.__name__, attrs)[:1000] # cap at 1K characters

        @recursive_repr()
        def __str__(self):
            attrs = "\n".join(
                "    {} = {}".format(k, v)
                    for k, v in self.__dict__.items()
                    if include_str_attr(k)
            )
            return "\n{}: {}\n{}".format(self.__class__.__name__, id(self), attrs)[:1000] # cap at 1K characters

        cls.__repr__ = __repr__
        cls.__str__ = __str__
        return cls

    if str_attrs or repr_attrs:
        return wrapper
    else:
        cls = args[0]
        return wrapper(cls)


def pprint_sql(cmd) -> str:
    # dialect = cmd.get_bind().dialect
    try:
        return sqlparse.format(
                '\n' + str(cmd.compile(dialect=sqlite.dialect(),
                                       compile_kwargs={"literal_binds": True})),
                reindent=True)
    except:
        try:
            return "Unable to represent bindings:\n{}".format(
                sqlparse.format(
                    str(cmd.compile(dialect=sqlite.dialect())),
                    reindent=True)
            )
        except Exception as e:
            return "utilities.db: Could not parse sql query; error: {}" \
                   .format(str(e))


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

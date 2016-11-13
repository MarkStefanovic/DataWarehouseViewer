import datetime
import re

from hypothesis import assume
from math import isinf, isnan
from pytest import (
    fixture,
    main
)
from hypothesis import given
from hypothesis.extra.datetime import (
    dates,
    datetimes
)
from hypothesis.strategies import (
    booleans,
    floats,
    integers,
    text
)

from star_schema.constellation import (
    convert_value,
    Field, format_value)
from star_schema.custom_types import FieldType, FieldFormat


@fixture(scope='module')
def integer_field():
    return Field(name='test_int_field',
                 dtype=FieldType.Int,
                 display_name='Test Int Field',
                 field_format=FieldFormat.Int
    )


@fixture(scope='module')
def string_field():
    return Field(name='test_str_field',
                 dtype=FieldType.Str,
                 display_name='Test String Field',
                 field_format=FieldFormat.Str
    )


@fixture(scope='module')
def bool_field():
    return Field(name='test_bool_field',
                 dtype=FieldType.Bool,
                 display_name='Test Boolean Field',
                 field_format=FieldFormat.Bool
    )


@fixture(scope='module')
def float_field():
    return Field(name='test_float_field',
                 dtype=FieldType.Float,
                 display_name='Test Float Field',
                 field_format=FieldFormat.Float
    )


@fixture(scope='module')
def date_field():
    return Field(name='test_date_field',
                 dtype=FieldType.Date,
                 display_name='Test Date Field',
                 field_format=FieldFormat.Date
    )


@given(x=integers())
def test_convert_value_int_field_returns_int(x, integer_field):
    """Test that converting a value to an integer returns an integer or None"""
    assert type(convert_value(field_type=integer_field.dtype, value=x)) == int


@given(x=text())
def test_convert_value_str_field_returns_str(x, string_field):
    """Test that converting a value to an str returns an str"""
    assert type(convert_value(field_type=string_field.dtype, value=x)) == str


@given(x=booleans())
def test_convert_value_bool_field_returns_bool(x, bool_field):
    """Test that converting a value to a bool returns a bool or None"""
    assert type(convert_value(field_type=bool_field.dtype, value=x)) == bool


@given(x=floats())
def test_convert_value_float_field_returns_float(x, float_field):
    """Test that converting a value to a float returns an float or None"""
    conv = convert_value(field_type=float_field.dtype, value=x)
    if x is None:
        assert conv is None
    elif isinf(x) or isnan(x):
        assert conv is None
    else:
        assert type(conv) == float


@given(x=datetimes())
def test_convert_value_date_field_returns_date_or_none(x, date_field):
    """Test that converting a value to an date returns an date or None"""
    assert type(convert_value(field_type=date_field.dtype, value=x)) == datetime.datetime
    dt = x.date()
    assert type(convert_value(field_type=date_field.dtype, value=dt)) == datetime.date


@given(x=integers())
def test_format_value_integers(x, integer_field):
    """Test that all integers are properly formatted and that the value is unchanged"""
    fmt_val = format_value(field_type=integer_field.dtype,
                           value=x,
                           field_format=integer_field.field_format)
    assert fmt_val == '{: d}'.format(x)
    assert int(fmt_val) == x
    assert re.match(r'^[\s\-]\d+', fmt_val)


@given(x=datetimes())
def test_format_value_dates(x, date_field):
    """Test that all dates are properly formatted and that the value is unchanged"""
    fmt_val = format_value(field_type=date_field.dtype,
                           value=x,
                           field_format=date_field.field_format)
    assert re.match(r'^\d{4}-\d{2}-\d{2}$', fmt_val)


@given(x=text())
def test_format_value_strings(x, string_field):
    """Test that all strings are properly formatted and that the value is unchanged"""
    fmt_val = format_value(field_type=string_field.dtype,
                           value=x,
                           field_format=string_field.field_format)
    assert fmt_val == '{}'.format(x)
    assert str(fmt_val) == x


@given(x=floats())
def test_format_value_floats(x, float_field):
    """Test that all floats are properly formatted and that the value is unchanged"""
    fmt_val = format_value(field_type=float_field.dtype,
                           value=x,
                           field_format=float_field.field_format)
    if x is None:
        assert fmt_val is None
    elif isinf(x):
        assert fmt_val is None
    elif isnan(x):
        assert fmt_val is None
    else:
        v = float(fmt_val.replace(',', ''))
        abs_diff = abs(v - x)
        assert abs_diff <= 0.01
        assert re.match(r'^[\s\-].*\d[.]\d+', fmt_val)


if __name__ == '__main__':
    # dt = datetimes().example()
    # print(dt.date())
    main(__file__)

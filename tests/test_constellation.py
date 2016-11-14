import datetime
import re

import pytest
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
    format_value,
    CalculatedField)


class TestConvertValue:
    @given(x=integers())
    def test_convert_value_int_field_returns_int(self, x, integer_field):
        """Converting a value to an integer returns an integer or None"""
        assert type(convert_value(field_type=integer_field.dtype, value=x)) == int

    @given(x=text())
    def test_convert_value_str_field_returns_str(self, x, string_field):
        """Converting a value to an str returns an str"""
        assert type(convert_value(field_type=string_field.dtype, value=x)) == str

    @given(x=booleans())
    def test_convert_value_bool_field_returns_bool(self, x, bool_field):
        """Converting a value to a bool returns a bool or None"""
        assert type(convert_value(field_type=bool_field.dtype, value=x)) == bool

    @given(x=floats())
    def test_convert_value_float_field_returns_float(self, x, float_field):
        """Converting a value to a float returns an float or None"""
        conv = convert_value(field_type=float_field.dtype, value=x)
        if x is None:
            assert conv is None
        elif isinf(x) or isnan(x):
            assert conv is None
        else:
            assert type(conv) == float

    @given(x=datetimes())
    def test_convert_value_date_field_returns_date_or_none(self, x, date_field):
        """Converting a value to an date returns an date or None"""
        assert type(convert_value(field_type=date_field.dtype, value=x)) == datetime.datetime
        dt = x.date()
        assert type(convert_value(field_type=date_field.dtype, value=dt)) == datetime.date


class TestFormatValue:
    @given(x=integers())
    def test_format_value_integers(self, x, integer_field):
        """Integers are properly formatted and that the value is unchanged"""
        fmt_val = format_value(field_type=integer_field.dtype,
                               value=x,
                               field_format=integer_field.field_format)
        assert fmt_val == '{: d}'.format(x)
        assert int(fmt_val) == x
        assert re.match(r'^[\s\-]\d+', fmt_val)


    @given(x=datetimes())
    def test_format_value_dates(self, x, date_field):
        """Dates are properly formatted and that the value is unchanged"""
        fmt_val = format_value(field_type=date_field.dtype,
                               value=x,
                               field_format=date_field.field_format)
        assert re.match(r'^\d{4}-\d{2}-\d{2}$', fmt_val)


    @given(x=text())
    def test_format_value_strings(self, x, string_field):
        """Strings are properly formatted and that the value is unchanged"""
        fmt_val = format_value(field_type=string_field.dtype,
                               value=x,
                               field_format=string_field.field_format)
        assert fmt_val == '{}'.format(x)
        assert str(fmt_val) == x


    @given(x=floats())
    def test_format_value_floats(self, x, float_field):
        """Floats are properly formatted and that the value is unchanged"""
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


class TestCalculatedField:
    @classmethod
    def create_calculated_field(self, formula: str):
        return CalculatedField(
            formula=formula,
            display_name='Test Calculated Field',
            show_on_fact_table=True
        )

    @pytest.fixture(scope='module')
    def int_calculated_field(self):
        fld = self.create_calculated_field(
            formula="(([1] + ([2] - [3])) - [2]) / ([3] - [2])")
        fld.base_field_lkp = {}
        return fld

    @pytest.fixture(scope='module')
    def text_calculated_field(self):
        fld = self.create_calculated_field(
            formula="([Apple] + [Pear]) / ([Tree] - [Pear])")
        fld.base_field_lkp = {
            'Apple': 2,
            'Pear': 3,
            'Tree': 4
        }
        return fld

    @pytest.fixture(scope='module')
    def mixed_calculated_field(self):
        fld = self.create_calculated_field(
            formula="([Apple] + [1.2]) / ([Tree] - [Pear])")
        fld.base_field_lkp = {
            'Apple': 2,
            'Pear': 3,
            'Tree': 4
        }
        return fld

    def test_parsed_formula_numeric_formula(self, int_calculated_field):
        assert int_calculated_field.parsed_formula == \
               [[['1', '+', ['2', '-', '3']], '-', '2'], '/', ['3', '-', '2']]

    def test_parsed_formula_text_formula(self, text_calculated_field):
        assert text_calculated_field.parsed_formula == \
               [['Apple', '+', 'Pear'], '/', ['Tree', '-', 'Pear']]

    def test_parsed_formula_mixed_formula(self, mixed_calculated_field):
        assert mixed_calculated_field.parsed_formula == \
               [['Apple', '+', '1.2'], '/', ['Tree', '-', 'Pear']]

    def test_evaluate_expression_numeric_formula(self, int_calculated_field):
        assert int_calculated_field.evaluate_expression == -2.0

    def test_evaluate_expression_text_formula(self, text_calculated_field):
        assert text_calculated_field.evaluate_expression == 5

    def test_evaluate_expression_mixed_formula(self, mixed_calculated_field):
        assert mixed_calculated_field.evaluate_expression == 3.2


if __name__ == '__main__':
    # dt = datetimes().example()
    # print(dt.date())
    main(__file__)

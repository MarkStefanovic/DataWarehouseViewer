from pytest import fixture

from star_schema.constellation import Field, CalculatedField
from star_schema.custom_types import FieldType, FieldFormat


@fixture(scope='module')
def integer_field():
    return Field(name='test_int_field',
                 dtype=FieldType.Int,
                 display_name='Test Int Field',
                 field_format=FieldFormat.Int)


@fixture(scope='module')
def string_field():
    return Field(name='test_str_field',
                 dtype=FieldType.Str,
                 display_name='Test String Field',
                 field_format=FieldFormat.Str)


@fixture(scope='module')
def bool_field():
    return Field(name='test_bool_field',
                 dtype=FieldType.Bool,
                 display_name='Test Boolean Field',
                 field_format=FieldFormat.Bool)


@fixture(scope='module')
def float_field():
    return Field(name='test_float_field',
                 dtype=FieldType.Float,
                 display_name='Test Float Field',
                 field_format=FieldFormat.Float)


@fixture(scope='module')
def date_field():
    return Field(name='test_date_field',
                 dtype=FieldType.Date,
                 display_name='Test Date Field',
                 field_format=FieldFormat.Date)


# @fixture(scope='module')
# def calculated_field():
#     return CalculatedField(
#         formula='1 + 2',
#         display_name='Test Calculated Field',
#         show_on_fact_table=True
#     )

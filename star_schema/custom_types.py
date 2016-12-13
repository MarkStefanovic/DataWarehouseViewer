"""The entities in this module are used to support MyPy type checking and to
improve the readability of the code."""

import datetime
from collections import namedtuple
from enum import unique, Enum

from typing import (
    NewType,
    Union,
    Optional,
    Callable,
    Tuple,
    Any
)

from star_schema.utilities import autorepr

ColumnIndex = NewType('ColumnIndex', int)
DateString = NewType('DateString', str)
DimensionTableName = NewType('DimensionTableName', str)
DimensionDisplayName = NewType('DimensionDisplayName', str)
ErrorMessage = NewType('ErrorMessage', str)
FactTableName = NewType('FactTableName', str)
FieldIndex = NewType('FieldType', int)
LookupTableName = NewType('LookupTableName', str)
TableName = NewType('TableName', str)
TableDisplayName = NewType('TableDisplayName', str)
ViewName = NewType('ViewName', str)
FieldName = NewType('FieldName', str)
FieldDisplayName = NewType('FieldDisplayName', str)
ForeignKeyValue = NewType('ForeignKeyValue', int)
PrimaryKeyIndex = NewType('PrimaryKeyIndex', int)
PrimaryKeyValue = NewType('RowID', int)

SqlDataType = Union[bool, str, int, float, datetime.date, datetime.datetime]
# Validator = Callable[[...], Tuple[bool, Optional[ErrorMessage]]]
Validator = Callable[[Any, ErrorMessage], Tuple[bool, Optional[ErrorMessage]]]

DisplayField = namedtuple(
    'DisplayField',
    'display_index '
    'original_index '
    'name '
    'display_name '
    'field_type '
    'dtype '
    'field_format '
    'editable '
    'visible '
    'dimension '
    'primary_key'
)


@unique
class FieldType(Enum):
    """Predefined field data types"""
    Date = "date"
    Float = "float"
    Int = "integer"
    Str = "string"
    Bool = "boolean"

    def __init__(self, data_type) -> None:
        self.data_type = data_type

    def __str__(self):
        return str(self.value)


@unique
class FieldFormat(Enum):
    """This class is used by the rows manager when processing the list to
    represent the data for display."""
    Accounting = "accounting"
    Bool = "boolean"
    Currency = "currency"
    Date = "date"
    DateTime = "datetime"
    Float = "float"
    Int = "integer"
    Str = "string"
    Memo = "memo"

    def __init__(self, field_format) -> None:
        self.field_format = field_format

    def __str__(self):
        return str(self.value)


@unique
class Operator(Enum):
    """Predefined operator types"""
    bool_is = "Is"
    bool_is_not = "Is Not"  # only useful if the bool field allows Null

    number_equals = "Equals"
    number_does_not_equal = "Doesn't Equal"
    number_greater_than = "Greater Than"
    number_greater_than_or_equal_to = "Greater Than or Equal To"
    number_less_than = "Less Than"
    number_less_than_or_equal_to = "Less Than or Equal To"

    str_equals = ""
    str_like = "Like"
    str_not_like = "Not Like"
    str_starts_with = "Starts With"
    str_ends_with = "Ends With"

    date_after = "After"
    date_on_or_after = "On or After"
    date_before = "Before"
    date_on_or_before = "On or Before"
    date_equals = "On"
    date_does_not_equal = "Not On"

    @property
    def suffix(self) -> str:
        return self.value


class SortOrder(Enum):
    """Predefined sort order options"""
    Ascending = "Ascending"
    Descending = "Descending"

    def __str__(self):
        return str(self.value)


@autorepr
class OrderBy:
    """Configuration for a sort order field"""
    def __init__(self, *,
        field_name: FieldName,
        sort_order: SortOrder=SortOrder.Ascending
    ) -> None:
        self.sort_order = sort_order
        self.field_name = field_name


class VerticalAlignment(Enum):
    """Predefined vertical alignment options for display"""
    Bottom = "bottom"
    Center = "vertical center"
    Top = "top"

    def __str__(self):
        return str(self.value)


class HorizontalAlignment(Enum):
    """Predefined horizontal alignment options for display"""
    Center = "horizontal center"
    Left = "left"
    Right = "right"

    def __str__(self):
        return str(self.value)


"""The entities in this module are used to support MyPy type checking and to
improve the readability of the code."""

import datetime
import re
from enum import unique, Enum

from typing import (
    NewType,
    Union,
    Optional, Callable, Tuple, Any)

from star_schema.utilities import autorepr

ColumnIndex = NewType('ColumnIndex', int)
DateString = NewType('DateString', str)
DimensionName = NewType('DimensionName', str)
ErrorMessage = NewType('ErrorMessage', str)
FactName = NewType('FactName', str)
FieldIndex = NewType('FieldType', int)
TableName = NewType('TableName', str)
ViewName = NewType('ViewName', str)
FieldName = NewType('FieldName', str)
ForeignKeyValue = NewType('ForeignKeyValue', int)
PrimaryKeyIndex = NewType('PrimaryKeyIndex', int)

SqlDataType = Union[bool, str, int, float, datetime.date, datetime.datetime]
# Validator = Callable[[...], Tuple[bool, Optional[ErrorMessage]]]
Validator = Callable[[Any, ErrorMessage], Tuple[bool, Optional[ErrorMessage]]]


class DateStr(str):
    def __new__(cls, content) -> DateString:
        if not content:
            return super().__new__(cls, '')
        if isinstance(content, str):
            if re.match(r"^\d{4}-\d{2}-\d{2}.*$", content):
                return super().__new__(cls, content[:10])
            raise ValueError("{v} is not a valid date".format(v=content))
        return str(content)[:10]

    @staticmethod
    def convert_to_datetime(val: DateString) -> datetime.date:
        if re.match(r"^\d{4}-\d{2}-\d{2}.*$", val):
            return datetime.datetime.strptime(val[:10], "%Y-%m-%d").date()
        raise ValueError("{v} is not a valid date".format(v=val))


@unique
class FieldType(Enum):
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

    def __init__(self, field_format) -> None:
        self.field_format = field_format

    def __str__(self):
        return str(self.value)


@unique
class Operator(Enum):
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
    Ascending = "Ascending"
    Descending = "Descending"

    def __str__(self):
        return str(self.value)

@autorepr
class OrderBy:
    """This class stores the configuration for a sort order field"""
    def __init__(self, *,
            field_name: FieldName,
            sort_order: SortOrder=SortOrder.Ascending
    ) -> None:
        self.sort_order = sort_order
        self.field_name = field_name


class VerticalAlignment(Enum):
    Bottom = "bottom"
    Center = "vertical center"
    Top = "top"

    def __str__(self):
        return str(self.value)


class HorizontalAlignment(Enum):
    Center = "horizontal center"
    Left = "left"
    Right = "right"

    def __str__(self):
        return str(self.value)


@autorepr
class FilterConfig:
    """This class serves as a temporary holding tank for would-be Filter configs"""
    def __init__(self, *,
            operator: Operator,
            default_value: Optional[SqlDataType]=''
    ) -> None:
        self.default_value = default_value
        self.operator = operator

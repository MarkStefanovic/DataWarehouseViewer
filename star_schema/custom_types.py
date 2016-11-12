"""The entities in this module are used to support MyPy type checking and to
improve the readability of the code."""

import datetime
import re
from enum import unique, Enum

from typing import (
    NewType,
    Union,
    Optional)

from star_schema.utilities import autorepr

ColumnIndex = NewType('ColumnIndex', int)
DateString = NewType('DateString', str)
DimensionName = NewType('DimensionName', str)
FactName = NewType('FactName', str)
FieldIndex = NewType('FieldType', int)
TableName = NewType('TableName', str)
ViewName = NewType('ViewName', str)
FieldName = NewType('FieldName', str)
ForeignKeyValue = NewType('ForeignKeyValue', int)
PrimaryKeyIndex = NewType('PrimaryKeyIndex', int)

SqlDataType = Union[bool, str, int, float, datetime.date, datetime.datetime]


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
    Date = 1
    Float = 2
    Int = 3
    Str = 4
    Bool = 5

    def __init__(self, data_type) -> None:
        self.data_type = data_type


@unique
class FieldFormat(Enum):
    """This class is used by the rows manager when processing the list to
    represent the data for display."""
    Accounting = 1
    Bool = 2
    Currency = 3
    Date = 4
    DateTime = 5
    Float = 6
    Int = 7
    Str = 8

    def __init__(self, field_format) -> None:
        self.field_format = field_format


@unique
class Operator(Enum):
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
    Ascending = 1
    Descending = 2


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
    Bottom = 1
    Center = 2
    Top = 3


class HorizontalAlignment(Enum):
    Center = 1
    Left = 2
    Right = 3


@autorepr
class FilterConfig:
    """This class serves as a temporary holding tank for would-be Filter configs"""
    def __init__(self, *,
            operator: Operator,
            default_value: Optional[SqlDataType]=''
    ) -> None:
        self.default_value = default_value
        self.operator = operator

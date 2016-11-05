"""The entities in this module are used to support MyPy type checking and to
improve the readability of the code."""

import datetime
import re
from typing import (
    NewType,
    Union
)

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

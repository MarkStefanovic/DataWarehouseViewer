import datetime

from typing import (
    NewType,
    Union
)

ColumnIndex = NewType('ColumnIndex', int)
DateString = NewType('DateString', str)
DimensionName = NewType('DimensionName', str)
FactName = NewType('FactName', str)
ForeignKeyValue = NewType('ForeignKeyValue', int)
PrimaryKeyIndex = NewType('PrimaryKeyIndex', int)

SqlDataType = Union[bool, str, int, float, datetime.date, datetime.datetime]

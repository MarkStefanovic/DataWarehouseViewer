import datetime

from typing import (
    NewType,
    Union
)

ColumnIndex = NewType('ColumnIndex', int)
PrimaryKeyIndex = NewType('PrimaryKeyIndex', int)

SqlDataType = Union[bool, str, int, float, datetime.date, datetime.datetime]

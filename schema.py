"""The classes declared in this module are used by multiple modules within the project.


Table (Fact|Dimension) -> Filter -> Field + Operator
"""
from datetime import datetime
from enum import Enum, unique
from functools import reduce
import re


from typing import (
    Dict,
    List,
    Optional,
    Union
)
from utilities import autorepr, immutable_property

import sqlalchemy as sqa
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.dml import (
    Delete,
    Insert,
    Update
)
from logger import log_error


class date(str):
    def __new__(cls, content) -> str:
        if not content:
            return super().__new__(cls, '')
        if isinstance(content, str):
            if re.match(r"^\d{4}-\d{2}-\d{2}.*$", content):
                return super().__new__(cls, content[:10])
            raise ValueError("{v} is not a valid date".format(v=content))
        return str(content)[:10]

    @staticmethod
    def convert_to_datetime(val):
        if re.match(r"^\d{4}-\d{2}-\d{2}.*$", val):
            return datetime.strptime(val[:10], "%Y-%m-%d").date()
        raise ValueError("{v} is not a valid date".format(v=val))


@unique
class FieldType(Enum):
    date = date
    float = float
    int = int
    str = str
    bool = bool  # TODO -- add checkbox controls on form too

    @log_error
    def convert(self, value: str):
        return self.value(value)


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


@unique
class FieldFormat(Enum):
    currency = '${: ,.2f}'  # add commas, pad a space for negatives, 2 decimals
    date = '{:%Y-%m-%d}'
    datetime = '{:%Y-%m-%d %H:%M}'
    float = '{:,.4f}'  # show 4 decimal places
    int = '{: d}' # pad a space for negative numbers
    str = '{:s}'  # cut off at 40 characters for display


@autorepr
class Field:
    def __init__(self, *,
        name: str,
        dtype: FieldType,
        display_name: str,
        field_format: Optional[FieldFormat]=None,
        filter_operators: Optional[List[Operator]]=None,
        editable: bool=False,
        primary_key: bool=False
    ) -> None:

        self.name = name
        self.dtype = dtype
        self.display_name = display_name
        self.field_format = field_format or self.default_format
        self.editable = editable
        self.primary_key = primary_key
        self.filter_operators = filter_operators

    @property
    def default_format(self) -> FieldFormat:
        defaults = {
            FieldType.date: FieldFormat.date,
            FieldType.int: FieldFormat.int,
            FieldType.float: FieldFormat.currency,
            FieldType.str: FieldFormat.str
        }
        return defaults[self.dtype]

    def format_value(self, value):
        try:
            val = self.dtype.convert(value)
            return self.field_format.value.format(val)
        except:
            return value

    @property
    def schema(self):
        """Map the field to a sqlalchemy Column"""
        type_map = {
            FieldType.date: sqa.Date,
            FieldType.float: sqa.Float,
            FieldType.int: sqa.Integer,
            FieldType.str: sqa.String
        }
        return sqa.Column(
            self.name,
            type_=type_map[self.dtype](),
            primary_key=self.primary_key
        )


@autorepr
class Filter:

    def __init__(self, *, field: Field, operator: Operator) -> None:
        self.field = field
        self.operator = operator
        self.value = ''

    @property
    def display_name(self) -> str:
        suffix = self.operator.suffix
        return self.field.display_name + (" " + suffix if suffix else "")

    @property
    def filter(self) -> BinaryExpression:
        fld = self.field.schema
        operator_mapping = {
            Operator.number_equals: fld == self.value,
            Operator.number_does_not_equal: fld != self.value,
            Operator.number_greater_than: fld > self.value,
            Operator.number_greater_than_or_equal_to: fld >= self.value,
            Operator.number_less_than: fld < self.value,
            Operator.number_less_than_or_equal_to: fld <= self.value,

            Operator.str_equals: fld == self.value,
            Operator.str_like: fld.contains(self.value),
            Operator.str_not_like: fld.notlike('%{}%'.format(self.value)),
            Operator.str_starts_with: fld.startswith(self.value),
            Operator.str_ends_with: fld.endswith(self.value),

            Operator.date_after: sqa.func.date(fld) > self.value,
            Operator.date_on_or_after: sqa.func.date(fld) >= self.value,
            Operator.date_before: sqa.func.date(fld) < self.value,
            Operator.date_on_or_before: sqa.func.date(fld) <= self.value,
            Operator.date_equals: sqa.func.date(fld) == self.value,
            Operator.date_does_not_equal: sqa.func.date(fld) != self.value
        }
        if self.value:
            return operator_mapping[self.operator]

    def set_value(self, value: str):
        """The slot that the associated filter control sends messages to."""
        self.value = value


@autorepr
class Table:
    """A container to store fields

    This class is meant to be subclassed by Dimension and Fact table classes.
    """
    def __init__(self,
        table_name: str,
        display_name: str,
        fields: List[Field],
        editable: bool,
    ) -> None:

        self.table_name = table_name
        self.display_name = display_name
        self.fields = fields
        self.editable = editable

        self.filters = [
            Filter(field=fld, operator=op)
            for fld in self.fields if fld.filter_operators
            for op in fld.filter_operators
        ]

    def add_row(self, values: List[str]) -> Insert:
        """Statement to add a row to the table given a list of values
        """
        return self.schema.insert()

    def delete_row(self, id: int) -> Delete:
        """Statement to delete a row from the table given the primary key value."""
        return self.schema.delete().where(self.primary_key == id)

    def field(self, name) -> Field:
        return next(fld for fld in self.fields if fld.name == name)

    @property
    def foreign_keys(self) -> Dict[int, Field]:
        return {i: fld for i, fld in enumerate(self.fields) if isinstance(fld, ForeignKey)}

    @property
    def primary_key(self) -> Field:
        return next(c for c in self.schema.columns if c.primary_key == True)

    @property
    def primary_key_index(self) -> int:
        return next(i for i, c in enumerate(self.schema.columns) if c.primary_key == True)

    @immutable_property
    def schema(self):
        """Map table to a sqlalchemy table schema"""
        md = sqa.MetaData()
        cols = [fld.schema for fld in self.fields]
        return sqa.Table(self.table_name, md, *cols)

    def select(self, max_rows: int=1000) -> Select:
        s = self.schema.select()
        for f in self.filters:
            if f.value:
                s = s.where(f.filter)
        return s.limit(max_rows)

    def update_row(self, id: int, values: List[str]) -> Update:
        """Statement to update a row on the table given the primary key value."""
        for i, v in enumerate(values):
            if self.fields[i].dtype == FieldType.date:
                values[i] = FieldType.date.value.convert_to_datetime(v)
        return self.schema.update().where(self.primary_key == id).values(values)


@autorepr
class SummaryField(Field):
    """Concatenate multiple fields

    This field type is used for display on associated fact tables in lieu of
    their integer primary key.
    """
    def __init__(self, *,
        display_fields: Union[List[str], str],
        display_name: str,
        separator: str=' '
    ) -> None:

        field_def = "||'" + separator + "'||".join(display_fields)
        super(SummaryField, self).__init__(
            name=field_def,
            dtype=FieldType.str,
            display_name=display_name,
            editable=False,
            primary_key=False
        )

        self.display_fields = display_fields
        self.display_name = display_name
        self.separator = separator


@autorepr
class Dimension(Table):
    """Dimension table specifications

    We don't specify the maximum display or export rows since dimension tables
    should be (and in this case *must* have a low row count, and the user must
    be able to see the entire dimension to edit any foreign keys that may show
    up on the associated Fact table.
    """
    def __init__(self, *,
        table_name: str,
        display_name: str,
        fields: List[Field],
        summary_field: SummaryField,
        editable: bool=False
    ) -> None:

        super(Dimension, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            editable=editable
        )

        self.summary_field = summary_field

    @property
    def foreign_key_schema(self):
        display_fields = [
            self.field(n).schema
            for n in self.summary_field.display_fields
        ]
        summary_field = reduce(
            lambda x, y: x + self.summary_field.separator + y, display_fields
            ).label(self.summary_field.display_name)
        return sqa.select([self.primary_key, summary_field])


@autorepr
class ForeignKey(Field):
    def __init__(self, *,
        name: str,
        display_name: str,
        dimension: str
    ):
        super(ForeignKey, self).__init__(
            name=name,
            dtype=FieldType.int,
            display_name=display_name,
            editable=False,
            primary_key=True
        )

        self.dimension = dimension

    @property
    def schema(self):
        """Map the field to a sqlalchemy Column"""
        return sqa.Column(
            self.name,
            None,
            sqa.ForeignKey("{t}.{f}".format(t=self.dimension, f=self.name))
        )


@autorepr
class Fact(Table):
    """Fact table specification

    Fact tables are generally long, but not wide.  They primarily contain data
    that is aggregated, and references to dimension tables for contextual data.
    They may also contain 'junk' dimensions, which are simply dimensions that
    don't warrant a seperate table to store them.
    """
    def __init__(self, *,
        table_name: str,
        display_name: str,
        fields: List[Field],
        editable: bool=False
    ) -> None:

        super(Fact, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            editable=editable
        )

    @property
    def dimensions(self):
        return [fld.dimension for fld in self.foreign_keys.values()]

# if __name__ == '__main__':
#     import doctest
#     doctest.ELLIPSIS_MARKER = '*etc*'
#     doctest.testmod()




"""The classes declared in this module are used by multiple modules within the project.

"""
from datetime import datetime
from enum import Enum, unique
from functools import reduce
from itertools import chain
import re
from sortedcollections import ValueSortedDict
from sqlalchemy import select
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


md = sqa.MetaData()


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

    def __init__(self, data_type):
        self.data_type = data_type

    def convert(self, value: str):
        if value:
            return self.data_type(value)
        return ''


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
    accounting = '{: ,.2f} '  # 2 decimal places, comma, pad for negatives, pad 1 right
    currency = '${: ,.2f} '  # 2 decimals, add commas, pad for negatives, pad 1 right
    date = '{:%Y-%m-%d}'
    datetime = '{:%Y-%m-%d %H:%M}'
    float = '{:,.4f}'  # show 4 decimal places
    int = '{: d}'  # pad a space for negative numbers
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

    @immutable_property
    def default_format(self) -> FieldFormat:
        defaults = {
            FieldType.date: FieldFormat.date,
            FieldType.int: FieldFormat.int,
            FieldType.float: FieldFormat.accounting,
            FieldType.str: FieldFormat.str
        }
        return defaults[self.dtype]

    def format_value(self, value):
        try:
            val = self.dtype.convert(value)
            return self.field_format.value.format(val)
        except:
            return value

    @immutable_property
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
        self._value = None

    @immutable_property
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

    def __lt__(self, other):
        return self.display_name < other.display_name

    @property
    def value(self):
        return self.field.dtype.convert(self._value)

    @value.setter
    def value(self, value: str):
        """The slot that the associated filter control sends messages to."""
        self._value = value


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

    def add_row(self, values: List[str]) -> Insert:
        """Statement to add a row to the table given a list of values
        """
        return self.schema.insert()

    def delete_row(self, id: int) -> Delete:
        """Statement to delete a row from the table given the primary key value."""
        return self.schema.delete().where(self.primary_key == id)

    def field(self, name) -> Field:
        return next(fld for fld in self.fields if fld.name == name)

    @immutable_property
    def filters(self):
        return [
            Filter(field=fld, operator=op)
            for fld in self.fields if fld.filter_operators
            for op in fld.filter_operators
        ]

    @immutable_property
    def foreign_keys(self) -> Dict[int, Field]:
        return {i: fld for i, fld in enumerate(self.fields) if isinstance(fld, ForeignKey)}

    @immutable_property
    def primary_key(self) -> Field:
        return next(c for c in self.schema.columns if c.primary_key == True)

    @immutable_property
    def primary_key_index(self) -> int:
        return next(i for i, c in enumerate(self.schema.columns) if c.primary_key == True)

    @immutable_property
    def schema(self):
        """Map table to a sqlalchemy table schema"""
        # md = sqa.MetaData()
        cols = [fld.schema for fld in self.fields]
        return sqa.Table(self.table_name, md, *cols)

    def update_row(self, *, pk: int, values: List[str]) -> Update:
        """Statement to update a row on the table given the primary key value."""
        for i, v in enumerate(values):
            if self.fields[i].dtype == FieldType.date:
                values[i] = FieldType.date.value.convert_to_datetime(v)
        return self.schema.update().where(self.primary_key == pk).values(values)


@autorepr
class SummaryField(Field):
    """Concatenate multiple fields

    This field type is used for display on associated fact tables in lieu of
    their integer primary key.
    """
    def __init__(self, *,
        display_fields: Union[List[str], str],
        display_name: str,
        separator: str=' ',
        filter_operators: List[Operator]=None
    ) -> None:

        # field_def = ("||'" + separator + "'||").join(display_fields)
        super(SummaryField, self).__init__(
            name="_".join(display_fields), #field_def,
            dtype=FieldType.str,
            display_name=display_name,
            filter_operators=filter_operators,
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

    @immutable_property
    def display_field_schemas(self):
        return [
            self.field(n).schema
            for n in self.summary_field.display_fields
        ]

    @immutable_property
    def foreign_key_schema(self):
        summary_field = reduce(
            lambda x, y: x + self.summary_field.separator + y, self.display_field_schemas
            ).label(self.summary_field.display_name)
        return sqa.select([self.primary_key, summary_field])

    def select(self, max_rows: int=1000) -> Select:
        """Only the dimension has a select method on the table class since
        the Fact table has to consider foreign keys so its select statement
        is composed at the Star level"""
        s = self.schema.select()
        for f in (flt for flt in self.filters if flt.value):
            s = s.where(f.filter)
        return s.limit(max_rows)

    @immutable_property
    def summary_field_schema(self):
        fld = Field(
            name=self.summary_field.display_name,
            display_name=self.summary_field.display_name,
            dtype=FieldType.str
        )
        fld.schema = reduce(
            lambda x, y: x + self.summary_field.separator + y, self.display_field_schemas
            ).label(self.summary_field.display_name)
        return fld


@autorepr
class ForeignKey(Field):
    def __init__(self, *,
        name: str,
        display_name: str,
        dimension: str,
        foreign_key_field: str
    ):
        super(ForeignKey, self).__init__(
            name=name,
            dtype=FieldType.int,
            display_name=display_name,
            filter_operators=None,
            editable=False,
            primary_key=True
        )

        self.dimension = dimension
        self.foreign_key_field = foreign_key_field # name of id field on dim

    @immutable_property
    def schema(self):
        return sqa.Column(
            self.name,
            None,
            sqa.ForeignKey("{t}.{f}".format(t=self.dimension, f=self.foreign_key_field))
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
        return [
            fld.dimension
            for fld in self.foreign_keys.values()
        ]


@autorepr
class Star:
    def __init__(self, fact: Fact, dimensions: List[Dimension]):
        self.fact = fact
        self._dimensions = dimensions

    @immutable_property
    def dimensions(self):
        return [
            dim for dim in self._dimensions
            if dim.table_name in [
                fld.dimension for fld in self.fact.fields
                if isinstance(fld, ForeignKey)
            ]
        ]

    @immutable_property
    def filters(self) -> List[Filter]:
        star_filters = []
        for dim in self.dimensions:
            for op in dim.summary_field.filter_operators:
                fk_filter = Filter(
                    field=dim.summary_field_schema,
                    operator=op
                )
                star_filters.append(fk_filter)
        for f in (flt for flt in self.fact.filters):
            star_filters.append(f)
        return sorted(star_filters)

    def select(self, max_rows: int = 1000) -> Select:
        """Override the Fact tables select method implementation to
        account for foreign key filters."""
        fact = self.fact.schema
        star = fact
        for dim in self.dimensions:
            star = star.join(dim.schema)
        qry = select(fact.columns).select_from(star)
        for f in [flt for flt in self.filters if flt.value]:
            qry = qry.where(f.filter)
        return qry.limit(max_rows)


class Constellation:
    def __init__(self, *,
            app,
            dimensions: Optional[List[Dimension]],
            facts: List[Fact]
    ) -> None:
        super(Constellation, self).__init__()

        self.app = app
        self.dimensions = dimensions
        self.facts = facts
        self._foreign_keys = {
            tbl.table_name: {}
            for tbl in dimensions
        }  # type: Dict[str, Dict[int, str]]

    @immutable_property
    def stars(self) -> List[Star]:
        return {
            fact.table_name: Star(fact=fact, dimensions=self.dimensions)
            for fact in self.facts
        }

    @immutable_property
    def tables(self):
        return chain(self.facts, self.dimensions)

    @property
    def foreign_key_lookups(self):
        return {
            tbl.table_name: tbl.foreign_key_schema
            for tbl in self.dimensions
        }

    def foreign_keys(self, dim: str) -> Dict[int, str]:
        if self._foreign_keys[dim]:
            return self._foreign_keys[dim]
        self.pull_foreign_keys(dim)
        return self._foreign_keys[dim]

    def pull_foreign_keys(self, dim: str) -> None:
        from db import fetch
        self._foreign_keys[dim] = ValueSortedDict({
            row[0]: str(row[1])
            for row in fetch(self.foreign_key_lookups[dim])
        })

    def star(self, fact_table: str) -> Star:
        """Return the specific star localized on a specific Star"""
        return self.stars[fact_table]


# if __name__ == '__main__':
#     import doctest
#     doctest.ELLIPSIS_MARKER = '*etc*'
#     doctest.testmod()




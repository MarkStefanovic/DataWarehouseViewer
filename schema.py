"""The classes declared in this module are used by multiple modules within the project.

"""
import datetime
from enum import Enum, unique
from functools import reduce
from itertools import chain
from sortedcollections import ValueSortedDict
from sqlalchemy import select
from typing import (
    Dict,
    List,
    Optional,
    Iterable
)

import sqlalchemy as sqa
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.dml import (
    Delete,
    Insert,
    Update
)

from custom_types import (
    ColumnIndex,
    Date,
    DimensionName,
    FactName,
    FieldName,
    ForeignKeyValue,
    PrimaryKeyIndex,
    SqlDataType
)
from utilities import autorepr, static_property

md = sqa.MetaData()


@unique
class FieldType(Enum):
    date = Date
    float = float
    int = int
    str = str
    bool = bool

    def __init__(self, data_type) -> None:
        self.data_type = data_type

    def convert(self, value: SqlDataType) -> SqlDataType:
        default_value = {
            Date:  '',
            float: 0.0,
            int:   0,
            str:   '',
            bool:  False
        }
        if value:
            return self.data_type(value)
        return default_value[self.data_type]


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
    """This class is used by the query manager when processing the list to
    represent the data for display."""
    accounting = '{: ,.2f} '  # 2 decimal places, comma, pad for negatives, pad 1 right
    bool = '{0}{1}'  # basic str
    currency = '${: ,.2f} '  # 2 decimals, add commas, pad for negatives, pad 1 right
    date = '{:%Y-%m-%d}'
    datetime = '{:%Y-%m-%d %H:%M}'
    float = '{:,.4f}'  # show 4 decimal places
    int = '{: d}'  # pad a space for negative numbers
    str = '{:s}'  # basic str


@autorepr
class Field:
    """Instances of this class represent a column in a database table."""
    def __init__(self, *,
            name: str,
            dtype: FieldType,
            display_name: str,
            field_format: Optional[FieldFormat] = None,
            filter_operators: Optional[List[Operator]] = None,
            editable: bool = False,
            primary_key: bool = False
    ) -> None:

        self.name = name
        self.dtype = dtype
        self.display_name = display_name
        self.field_format = field_format or self.default_format
        self.editable = editable
        self.primary_key = primary_key
        self.filter_operators = filter_operators

    @static_property
    def default_format(self) -> FieldFormat:
        """Default display formats per the FieldFormat enum"""

        defaults = {
            FieldType.date:  FieldFormat.date,
            FieldType.int:   FieldFormat.int,
            FieldType.float: FieldFormat.accounting,
            FieldType.str:   FieldFormat.str
        }
        return defaults[self.dtype]

    def format_value(self, value: SqlDataType) -> SqlDataType:
        try:
            val = self.dtype.convert(value)
            return self.field_format.value.format(val)
        except:
            return value

    @static_property
    def schema(self) -> sqa.Column:
        """Map the field to a sqlalchemy Column"""
        type_map = {
            FieldType.bool:  sqa.Boolean,
            FieldType.date:  sqa.Date,
            FieldType.float: sqa.Float,
            FieldType.int:   sqa.Integer,
            FieldType.str:   sqa.String
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
        self._value = None  # type: Optional[SqlDataType]

    @static_property
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

    def __lt__(self, other) -> bool:
        return self.display_name < other.display_name

    @property
    def value(self) -> SqlDataType:
        return self.field.dtype.convert(self._value)

    @value.setter
    def value(self, value: str) -> None:
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
            display_rows: int=10000,
            export_rows: int=500000
    ) -> None:

        self.table_name = table_name
        self.display_name = display_name
        self.fields = fields
        self.editable = editable
        self.display_rows = display_rows
        self.export_rows = export_rows

    def add_row(self, values: List[str]) -> Insert:
        """Statement to add a row to the table given a list of values"""
        return self.schema.insert()

    def delete_row(self, id: int) -> Delete:
        """Statement to delete a row from the table given the primary key value."""
        return self.schema.delete().where(self.primary_key == id)

    def field(self, name: str) -> Field:
        """Look up a field based on it's name on the table."""
        return next(fld for fld in self.fields if fld.name == name)

    @static_property
    def filters(self) -> List[Filter]:
        return [
            Filter(field=fld, operator=op)
            for fld in self.fields if fld.filter_operators
            for op in fld.filter_operators
        ]

    @static_property
    def foreign_keys(self) -> Dict[ColumnIndex, Field]:
        return {ColumnIndex(i): fld for i, fld in enumerate(self.fields) if
            isinstance(fld, ForeignKey)}

    @static_property
    def primary_key(self) -> Field:
        return next(c for c in self.schema.columns if c.primary_key == True)

    @static_property
    def primary_key_index(self) -> PrimaryKeyIndex:
        return PrimaryKeyIndex(
            next(i for i, c in enumerate(self.schema.columns) if c.primary_key))

    @static_property
    def schema(self) -> sqa.Table:
        """Map table to a sqlalchemy table schema"""
        cols = [fld.schema for fld in self.fields]
        return sqa.Table(self.table_name, md, *cols)

    def update_row(self, *,
            pk: PrimaryKeyIndex,
            values: List[SqlDataType]) -> Update:
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
            display_fields: List[str],
            display_name: str,
            separator: str = ' ',
            filter_operators: Optional[List[Operator]] = None
    ) -> None:
        super(SummaryField, self).__init__(
            name="_".join(display_fields),
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
            table_name: DimensionName,
            display_name: str,
            fields: List[Field],
            summary_field: SummaryField,
            editable: bool=False,
            display_rows: int=10000,
            export_rows: int=500000
    ) -> None:
        super(Dimension, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            editable=editable,
            display_rows=display_rows,
            export_rows=export_rows
        )

        self.summary_field = summary_field

    @static_property
    def display_field_schemas(self) -> List[sqa.Column]:
        return [
            self.field(n).schema
            for n in self.summary_field.display_fields
        ]

    @static_property
    def foreign_key_schema(self) -> Table:
        summary_field = reduce(
            lambda x, y: x + self.summary_field.separator + y,
            self.display_field_schemas
        ).label(self.summary_field.display_name)
        return sqa.select([self.primary_key, summary_field])

    def select(self, max_rows: int = 1000) -> Select:
        """Only the dimension has a select method on the table class since
        the Fact table has to consider foreign keys so its select statement
        is composed at the Star level"""
        s = self.schema.select()
        for f in (flt for flt in self.filters if flt.value):
            s = s.where(f.filter)
        return s.limit(max_rows)

    @static_property
    def summary_field_schema(self) -> List[sqa.Column]:
        fld = Field(
            name=self.summary_field.display_name,
            display_name=self.summary_field.display_name,
            dtype=FieldType.str
        )
        fld.schema = reduce(
            lambda x, y: x + self.summary_field.separator + y,
            self.display_field_schemas).label(self.summary_field.display_name)
        return fld


@autorepr
class ForeignKey(Field):
    def __init__(self, *,
            name: str,
            display_name: str,
            dimension: DimensionName,
            foreign_key_field: str
    ) -> None:
        super(ForeignKey, self).__init__(
            name=name,
            dtype=FieldType.int,
            display_name=display_name,
            filter_operators=None,
            editable=False,
            primary_key=True
        )

        self.dimension = dimension
        self.foreign_key_field = foreign_key_field  # name of id field on dim

    @static_property
    def schema(self) -> sqa.Column:
        return sqa.Column(
            self.name,
            None,
            sqa.ForeignKey("{t}.{f}".format(t=self.dimension,
                f=self.foreign_key_field))
        )


@autorepr
class Fact(Table):
    """Fact table specification

    Fact tables are generally long, but not wide.  They primarily contain data
    that is aggregated, and references to dimension tables for contextual data.
    They may also contain 'junk' dimensions, which are simply dimensions that
    don't warrant a separate table to store them.
    """

    def __init__(self, *,
            table_name: FactName,
            display_name: str,
            fields: List[Field],
            editable: bool=False,
            display_rows: int=10000,
            export_rows: int=500000
    ) -> None:
        super(Fact, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            editable=editable,
            display_rows=display_rows,
            export_rows=export_rows
        )

    @property
    def dimensions(self) -> List[DimensionName]:
        """List of the associated dimension names"""
        return [
            fld.dimension
            for fld in self.foreign_keys.values()
        ]


@autorepr
class Star:
    """A Star is a Fact table with associated Dimensions

    A Star is a view for a fact table.  It inherits its editability
    from its core star.
    """

    def __init__(self, *,
        fact: Fact,
        dimensions: List[Dimension]=None
    ) -> None:

        self.fact = fact
        self._dimensions = dimensions

    @static_property
    def dimensions(self) -> List[Dimension]:
        return [
            dim for dim in self._dimensions
            if dim.table_name in [
                fld.dimension for fld in self.fact.fields
                if isinstance(fld, ForeignKey)
            ]
        ]

    @property
    def editable(self) -> bool:
        return self.fact.editable

    @static_property
    def filters(self) -> List[Filter]:
        star_filters = []  # type: List
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

    @property
    def select(self) -> Select:
        """Override the Fact tables select method implementation to
        account for foreign key filters."""
        return self.star_query.limit(self.fact.display_rows)

    @property
    def star_query(self) -> Select:
        fact = self.fact.schema  # type: sqa.Table
        star = fact
        for dim in self.dimensions:
            star = star.join(dim.schema)
        qry = select(fact.columns).select_from(star)
        for f in [flt for flt in self.filters if flt.value]:
            qry = qry.where(f.filter)
        return qry


# @autorepr
# class AggregateView:
#     """An aggregate view over a Star"""
#
#     def __init__(self, *,
#             display_name: str,
#             fact_table_name: FactName,
#             group_by_field_display_names: List[FieldName],
#             aggregate_field_display_names: FieldName
#     ) -> None:
#
#         self._fact_table_name = fact_table_name
#
#     @static_property
#     def star(self) -> Star:
#         from config import cfg
#         return cfg.star[self._fact_table]
#
#     @static_property
#     def filters(self) -> List[Filter]:
#         return self.star.filters
#
#     @property
#     def select(self) -> Select:
#         return self.star.star_query


class Constellation:
    """Collection of all the Stars in the application"""

    def __init__(self, *,
            app,
            dimensions: List[Dimension],
            facts: List[Fact]
    ) -> None:
        self.app = app
        self.dimensions = dimensions  # List[Dimension]
        self.facts = facts  # type: List[Fact]
        self._foreign_keys = {
            tbl.table_name: {}
            for tbl in dimensions
        }  # type: Dict[str, Dict[int, str]]

    @static_property
    def stars(self) -> Dict[FactName, Star]:
        return {
            fact.table_name: Star(fact=fact, dimensions=self.dimensions)
            for fact in self.facts
        }

    @static_property
    def tables(self) -> List[Table]:
        return chain(self.facts, self.dimensions)

    @property
    def foreign_key_lookups(self) -> Dict[DimensionName, Select]:
        return {
            tbl.table_name: tbl.foreign_key_schema
            for tbl in self.dimensions
        }

    def foreign_keys(self, dim: DimensionName) -> Dict[ForeignKeyValue,
                                                       SqlDataType]:
        if self._foreign_keys[dim]:
            return self._foreign_keys[dim]
        self.pull_foreign_keys(dim)
        return self._foreign_keys[dim]

    def pull_foreign_keys(self, dim: DimensionName) -> None:
        select_statement = self.foreign_key_lookups[dim]  # type: Select
        from db import fetch
        self._foreign_keys[dim] = ValueSortedDict({
            row[0]: str(row[1])
            for row in fetch(select_statement)
        })

    def star(self, fact_table: FactName) -> Star:
        """Return the specific Star system localized on a specific Fact table"""

        return self.stars[fact_table]


# if __name__ == '__main__':
#     from config import cfg
#     print(type(cfg.stars('factSales').fact.schema))
#     import doctest
#     doctest.ELLIPSIS_MARKER = '*etc*'
#     doctest.testmod()



"""The classes declared in this module are used by multiple modules within the project.

"""
import datetime
from collections import ChainMap
from enum import Enum, unique
from functools import reduce
from itertools import chain
import re

from sortedcollections import ValueSortedDict
from sqlalchemy import select
from sqlalchemy import func
from sqlalchemy.sql import default_comparator  # needed by cx_freeze
from typing import (
    Dict,
    List,
    Optional,
    Union
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
    DimensionName,
    FactName,
    FieldName,
    ForeignKeyValue,
    PrimaryKeyIndex,
    SqlDataType,
    ViewName
)
from logger import log_error
from utilities import autorepr, static_property

md = sqa.MetaData()


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


def convert_value(*,
        field_type: FieldType,
        value: Optional[SqlDataType]=None
    ) -> SqlDataType:
    """Convert a string value to a Python data type

    This conversion function is used to translate user input to a form that
    sqlalchemy can use."""
    def convert_date(date_val: Optional[str]='1900-01-01'):
        try:
            if not date_val:
                return datetime.datetime(1900, 1, 1).date()
            if isinstance(date_val, str):
                if re.match(r"^\d{4}-\d{2}-\d{2}.*$", date_val):
                    return datetime.datetime.strptime(date_val[:10], "%Y-%m-%d").date()
                raise ValueError("{v} is not a valid date".format(v=date_val))
            elif isinstance(date_val, datetime.date):
                return date_val
            return '' #datetime.datetime(1900, 1, 1).date()
        except Exception as e:
            print("Error converting date value {}; type {}".format(date_val, type(date_val)))

    conversion_functions = {
        FieldType.Date:  convert_date,
        FieldType.Float: lambda v: round(float(v), 2),
        FieldType.Int:   int,
        FieldType.Str:   str,
        FieldType.Bool:  bool
    }
    default_values = {
        FieldType.Date:  '',
        FieldType.Float: 0.0,
        FieldType.Int:   0,
        FieldType.Str:   '',
        FieldType.Bool:  False
    }
    try:
        if not value:
            return default_values[field_type]
        return conversion_functions[field_type](value)
    except Exception as e:
        print('Error converting value {} to data type {}; err:'
            .format(value, field_type, str(e)))
        return default_values[field_type]


def format_value(*,
        field_type: FieldType,
        value: Optional[SqlDataType]=None,
        field_format: Optional[FieldFormat]=None
    ) -> SqlDataType:
    """Format a string value to a string appropriate for display to the user"""

    inferred_data_types = {
        FieldFormat.Accounting: FieldType.Float,
        FieldFormat.Bool:       FieldType.Bool,
        FieldFormat.Currency:   FieldType.Float,
        FieldFormat.Date:       FieldType.Date,
        FieldFormat.DateTime:   FieldType.Date,
        FieldFormat.Float:      FieldType.Float,
        FieldFormat.Int:        FieldType.Int,
        FieldFormat.Str:        FieldType.Str
    }
    data_type = inferred_data_types[field_type] if not field_type else field_type
    inferred_format = lambda fld_type: next(k for k, v in inferred_data_types.items() if v == field_type)
    format = inferred_format(field_type) if not field_format else field_format
    formatters = {
        FieldFormat.Accounting: lambda val: '{: ,.2f} '.format(round(val, 2)),
        FieldFormat.Bool:       lambda val: str(val),
        FieldFormat.Currency:   lambda val: '${: ,.2f} '.format(round(val, 2)),
        FieldFormat.Date:       lambda val: str(val),
        FieldFormat.DateTime:   lambda val: str(val),
        FieldFormat.Float:      lambda val: '{:,.4f}'.format(round(val, 2)),
        FieldFormat.Int:        lambda val: '{: d}'.format(round(val, 2)),
        FieldFormat.Str:        lambda val: val
    }
    default_display_values = {
        FieldType.Bool: False,
        FieldType.Float: 0.0,
        FieldType.Date: datetime.datetime(1900, 1, 1).date(),
        FieldType.Int: 0,
        FieldType.Str: ''
    }
    # this value is also saved to the db on empty inputs
    default_value = default_display_values[data_type]
    val = convert_value(field_type=data_type, value=value) if value else default_value
    try:
        return formatters[format](val)
    except Exception as e:
        print(
            'error formatting value,',
            'val:', value,
            'data_type:', data_type,
            'error msg:', str(e)
        )
        return value

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


@autorepr
class Field:
    """Instances of this class represent a column in a database table."""
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
        self.field_format = field_format
        self.editable = editable
        self.primary_key = primary_key
        self.filter_operators = filter_operators

    @static_property
    def schema(self) -> sqa.Column:
        """Map the field to a sqlalchemy Column"""
        type_map = {
            FieldType.Bool:  sqa.Boolean,
            FieldType.Date:  sqa.Date,
            FieldType.Float: sqa.Float,
            FieldType.Int:   sqa.Integer,
            FieldType.Str:   sqa.String
        }
        return sqa.Column(
            self.name,
            type_=type_map[self.dtype](),
            primary_key=self.primary_key
        )


@autorepr
class Filter:
    def __init__(self, *,
            field: Field,
            operator: Operator
    ) -> None:

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
        return convert_value(
            field_type=self.field.dtype,
            value=self._value
        )

    @value.setter
    def value(self, value: str) -> None:
        """The slot that the associated filter control sends messages to."""
        self._value = value


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
            show_on_load: bool=False,
            order_by: Optional[List[SortOrder]]=None,
            display_rows: int=10000,
    ) -> None:

        self.table_name = table_name
        self.display_name = display_name
        self.fields = fields
        self.editable = editable
        self.display_rows = display_rows
        self.order_by = order_by
        self.show_on_load = show_on_load

    def add_row(self, values: List[str]) -> Insert:
        """Statement to add a row to the table given a list of values"""

        # we want to use the primary key assigned by the db rather
        # than the one we auto-generated as a placeholder

        values_sans_pk = {
            fld.name: convert_value(value=values[i], field_type=self.fields[i].dtype)
            for i, fld in enumerate(self.fields)
            if not fld.primary_key
        }
        return self.schema.insert().values(values_sans_pk)

    def delete_row(self, id: int) -> Delete:
        """Statement to delete a row from the table given the primary key value."""
        return self.schema.delete().where(self.primary_key == id)

    def field(self, name: str) -> Field:
        """Look up a field based on it's name on the table."""
        try:
            return next(fld for fld in self.fields if fld.name == name)
        except StopIteration:
            print('could not find table field named {} on table {}'
                  .format(name, self.table_name))

    @static_property
    def filters(self) -> List[Filter]:
        return [
            Filter(field=fld, operator=op)
            for fld in self.fields if fld.filter_operators
            for op in fld.filter_operators
        ]

    @static_property
    def foreign_keys(self) -> Dict[ColumnIndex, Field]:
        return {
            ColumnIndex(i): fld
            for i, fld in enumerate(self.fields)
            if isinstance(fld, ForeignKey)
        }

    @static_property
    def primary_key(self) -> Field:
        try:
            return next(c for c in self.schema.columns if c.primary_key is True)
        except StopIteration:
            print('could not find the primary key for table {}'
                  .format(self.table_name))

    @static_property
    def primary_key_index(self) -> PrimaryKeyIndex:
        try:
            return PrimaryKeyIndex(
                next(i for i, c in enumerate(self.schema.columns)
                     if c.primary_key)
            )
        except StopIteration:
            print('could not find the primary key index for table {}'
                  .format(self.table_name))

    @static_property
    def schema(self) -> sqa.Table:
        """Map table to a sqlalchemy table schema"""
        try:
            cols = [fld.schema for fld in self.fields]
            return sqa.Table(self.table_name, md, *cols)
        except Exception as e:
            print('Error creating the schema for table {}; error: {}'
                  .format(self.table_name, str(e)))

    def update_row(self, *,
            pk: PrimaryKeyIndex,
            values: List[SqlDataType]
    ) -> Update:
        """Statement to update a row on the table given the primary key value."""
        converted_values = [
            convert_value(value=v, field_type=self.fields[i].dtype)
            for i, v in enumerate(values)
        ]
        return self.schema.update().where(self.primary_key == pk).values(converted_values)


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
            dtype=FieldType.Str,
            display_name=display_name,
            field_format=FieldFormat.Str,
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
            show_on_load: bool=True,
            order_by: Optional[List[OrderBy]]=None,
            display_rows: int=10000
    ) -> None:

        super(Dimension, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            editable=editable,
            show_on_load=show_on_load,
            display_rows=display_rows,
            order_by=order_by
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

    @static_property
    def order_by_schema(self):
        """The default sort order for the table"""

        def lkp_sort_order(
                fld_name: FieldName,
                sort_order: Optional[SortOrder]=None):

            fld = self.field(fld_name).schema
            if sort_order == SortOrder.Ascending:
                return fld.asc()
            return fld.desc()

        if self.order_by:
            return [
                lkp_sort_order(o.field_name, o.sort_order)
                for o in self.order_by
            ]

    @property
    def select(self, max_rows: int=1000) -> Select:
        """Only the dimension has a select method on the table class since
        the Fact table has to consider foreign keys so its select statement
        is composed at the Star level"""
        s = self.schema.select()
        for f in (flt for flt in self.filters if flt.value):
            s = s.where(f.filter)
        if self.order_by_schema:
            for o in self.order_by_schema:
                s = s.order_by(o)
        return s.limit(max_rows)

    @static_property
    def summary_field_schema(self) -> sqa.Column:
        fld = Field(
            name=self.summary_field.display_name,
            display_name=self.summary_field.display_name,
            dtype=FieldType.Str,
            field_format=FieldFormat.Str
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
            dtype=FieldType.Int,
            display_name=display_name,
            filter_operators=None,
            editable=False,
            primary_key=False
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
            show_on_load: bool=False,
            editable: bool=False,
            display_rows: int=10000,
            order_by: Optional[List[OrderBy]]=None
    ) -> None:

        super(Fact, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            show_on_load=show_on_load,
            editable=editable,
            display_rows=display_rows,
            order_by=order_by
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

    @static_property
    def display_rows(self) -> int:
        return self.fact.display_rows

    @property
    def editable(self) -> bool:
        return self.fact.editable

    @static_property
    def fields(self) -> Dict[FieldName, Field]:
        """Fields indexed by their display name"""
        fact_fields = {fld.display_name: fld for fld in self.fact.fields}
        all_fields = ChainMap({}, fact_fields, self.summary_fields)
        return all_fields

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

    @static_property
    def summary_fields(self) -> Dict[FieldName, Field]:
        return {
            str(dim.summary_field.display_name): dim.summary_field_schema.schema
            for dim in self.dimensions
        }  # type: Dict[FieldName, Field]

    @static_property
    def order_by(self):
        return self.fact.order_by

    @static_property
    def order_by_schema(self):
        """Return the order by fields for the Star"""
        if not self.order_by:
            return

        def lkp_sort_order(order_by: OrderBy):
            if order_by.field_name in self.summary_fields.keys():
                fld = self.summary_fields[order_by.field_name]
            else:
                fld = self.fact.field(order_by.field_name).schema
            if order_by.sort_order == SortOrder.Ascending:
                return fld.asc()
            return fld.desc()

        return [
            lkp_sort_order(o)
            for o in self.order_by
        ]

    @property
    def star_query(self):
        try:
            fact = self.fact.schema  # type: sqa.Table
            star = fact
            for dim in self.dimensions:
                star = star.outerjoin(dim.schema)
            qry = select(fact.columns).select_from(star)
            for f in [flt for flt in self.filters if flt.value]:
                qry = qry.where(f.filter)
            if self.order_by_schema:
                for o in self.order_by_schema:
                    qry = qry.order_by(o)
            return qry
        except Exception as e:
            print('error composing star query: {}'.format(str(e)))


@autorepr
class AdditiveField:
    """A field that represents an aggregate of a Fact.

    This field type is only used with Views over a Star.
    It mimics its base field except for the schema and editability"""

    def __init__(self, *,
        base_field_display_name: FieldName,
        aggregate_display_name: FieldName,
        aggregate_func: func=func.sum
    ) -> None:

        self.base_field_display_name = base_field_display_name
        self.display_name = aggregate_display_name
        self.aggregate_func = aggregate_func

        # The star property is injected by the Star itself later.
        # It must be populated before this field can be used.
        self.star = None

    @property
    def base_field(self) -> Field:
        if not self.star:
            raise Exception('AdditiveField must be assigned a star before use.')
        return self.star.fields[self.base_field_display_name]

    @property
    def dtype(self):
        """Mimic field property"""
        if self.aggregate_func._FunctionGenerator__names == ['count']:
            return FieldType.Int
        return self.base_field.dtype

    @property
    def editable(self):
        """Mimic field property"""
        return False

    @property
    def field_format(self):
        """Mimic field property"""
        if self.dtype == FieldType.Int:
            return FieldFormat.Int
        return FieldFormat.Accounting

    @property
    def filter_operators(self):
        """Mimic field property"""
        return self.base_field.filter_operators

    @property
    def name(self):
        """Mimic field property"""
        return self.base_field.name

    @property
    def primary_key(self):
        """Mimic field property"""
        return False

    @property
    def schema(self):
        try:
            return self.aggregate_func(self.base_field.schema).label(self.display_name)
        except Exception as e:
            print('error creating aggregate field {}; error: {}')

    @log_error
    def validate(self, star: Star):
        print('todo')


@autorepr
class View:
    """An aggregate view over a Star"""

    def __init__(self, *,
            view_display_name: str,
            fact_table_name: FactName,
            group_by_field_names: List[FieldName],
            additive_fields: Optional[List[AdditiveField]],
            show_on_load: bool=False
    ) -> None:

        self.display_name = view_display_name  # type: str
        self._fact_table_name = fact_table_name  # type: str
        self._group_by_fields = group_by_field_names  # type: Optional[List[FieldName]]
        self._additive_fields = additive_fields  # type: Optional[List[AdditiveField]]
        self.primary_key_index = -1
        self.editable = False
        self.show_on_load = show_on_load

    @static_property
    def additive_fields(self):
        for fld in self._additive_fields:
            fld.star = self.star
        return self._additive_fields


    @static_property
    def star(self) -> Star:
        """Get a reference to the Star associated with the current Fact table"""
        from config import cfg
        return cfg.star(fact_table=self._fact_table_name)

    @property
    def filters(self) -> List[Filter]:
        return self.star.filters

    @static_property
    def foreign_keys(self) -> Dict[ColumnIndex, Field]:
        return {
            i: fld
            for i, fld in enumerate(self.fields)
            if fld.name in [
                f.name
                for f in self.star.fact.foreign_keys.values()
            ]
        }

    @static_property
    def group_by_fields(self):
        return [
            self.star.fields[fld_name]
            for fld_name in self._group_by_fields
        ]

    @static_property
    def fields_schema(self):
        return [fld.schema for fld in self.group_by_fields] \
               + [fld.schema for fld in self.additive_fields]

    @static_property
    def fields(self):
        return self.group_by_fields + self.additive_fields

    @property
    def select(self) -> Select:
        try:
            star = self.star.fact.schema  # type: sqa.Table
            for dim in self.star.dimensions:
                star = star.outerjoin(dim.schema)
            qry = select(self.fields_schema).select_from(star)
            for f in [flt for flt in self.star.filters if flt.value]:
                qry = qry.where(f.filter)
            for g in self.group_by_fields:
                qry = qry.group_by(g.schema)
            if self.star.order_by_schema:
                for o in self.star.order_by_schema:
                    qry = qry.order_by(o)
            return qry.limit(self.star.display_rows)
        except Exception as e:
            print('error composing select statement for view {}; error {}'
                  .format(self.display_name, str(e)))


class Constellation:
    """Collection of all the Stars in the application"""

    def __init__(self, *,
            app,
            dimensions: Optional[List[Dimension]],
            facts: List[Fact],
            views: List[View]
    ) -> None:

        self.app = app
        self.dimensions = dimensions  # Optional[List[Dimension]]
        self.facts = facts  # type: List[Fact]
        self.views = views  # type Optional[List[View]]
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

    # @static_property
    # def views(self) -> Dict[ViewName, View]:
    #     return {
    #         view.display_name: view
    #         for view in self.views
    #     }

    @static_property
    def tables(self) -> List[Table]:
        return chain(self.facts, self.dimensions, self.views)

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
        fks = self._foreign_keys[dim]
        if not 0 in fks:
            self._foreign_keys[dim][0] = ""
            fks[0] = ""
        if not '' in fks:
            self._foreign_keys[dim][''] = ""
            fks[''] = ""
        return fks

    def pull_foreign_keys(self, dim: DimensionName) -> None:
        try:
            select_statement = self.foreign_key_lookups[dim]  # type: Select
            from db import fetch
            self._foreign_keys[dim] = ValueSortedDict({
                row[0]: str(row[1])
                for row in fetch(select_statement)
            })
        except Exception as e:
            print('error pulling foreign keys for dimension {}'
                  .format(dim))

    def star(self, fact_table: FactName) -> Star:
        """Return the specific Star system localized on a specific Fact table"""
        try:
            return self.stars[fact_table]
        except KeyError:
            print('The fact table {} could not be found in the cfg global variable.'
                  .format(fact_table))

    def view(self, view_name: ViewName) -> View:
        """Return the specified View"""
        try:
            return next(view for view in self.views if view.display_name == view_name)
        except StopIteration:
            print('A view with the display name {} could not be found in the '
                  'cfg global variable.'.format(view_name))
# if __name__ == '__main__':
#     from config import cfg
#     print(type(cfg.stars('factSales').fact.schema))
#     import doctest
#     doctest.ELLIPSIS_MARKER = '*etc*'
#     doctest.testmod()



import datetime
from collections import ChainMap
from itertools import chain
from math import isinf, isnan
import re

from functools import reduce

from sortedcollections import ValueSortedDict
from sqlalchemy import func
from sqlalchemy.sql import (
    Delete,
    Insert,
    Select,
    Update
)
from sqlalchemy.sql.elements import BinaryExpression, literal
from typing import Optional, List, Dict, Iterable, Any, Union

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    select,
    String
)
from sqlalchemy import Table as sqlaTable
from sqlalchemy import ForeignKey as sqlaForeignKey
from pyparsing import (
    Forward,
    oneOf,
    ZeroOrMore,
    Group,
    Literal,
    Word,
    Combine,
    alphanums,
    Regex
)
from pyparsing import Optional as pypOptional

from logger import rotating_log
from star_schema import md
from star_schema.custom_types import (
    FactName,
    DimensionName,
    ForeignKeyValue,
    SqlDataType,
    ViewName,
    FieldType,
    FieldFormat,
    FilterConfig,
    FieldName,
    Operator,
    SortOrder,
    ColumnIndex,
    PrimaryKeyIndex,
    OrderBy
)

from star_schema.utilities import (
    static_property,
    autorepr
)


date_str_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}.*$")
true_str_pattern = re.compile(".*true.*", re.IGNORECASE)
false_str_pattern = re.compile(".*false.*", re.IGNORECASE)
logger = rotating_log('constellation')


def convert_value(*,
        field_type: FieldType,
        value: Optional[SqlDataType]=None
    ) -> SqlDataType:
    """Convert a string value to a Python data type

    This conversion function is used to translate user input to a form that
    sqlalchemy can use."""
    default_values = {
        FieldType.Int:   0,
        FieldType.Str:   '',
        FieldType.Float: 0.0,
        FieldType.Bool:  False,
        FieldType.Date:  None
    }

    if value is None:
        return None
    if not value:
        return default_values[field_type]
    if isinstance(value, float):
        if isnan(value) or isinf(value):
            return None

    def convert_date(
            date_val: Optional[Union[str, datetime.date, datetime.datetime]]
        ) -> datetime.date:

        try:
            if not date_val:
                return
            if isinstance(date_val, str):
                if date_str_pattern.match(date_val):
                    return datetime.datetime.strptime(date_val[:10], "%Y-%m-%d").date()
                logger.debug(
                    "convert_value: {v} is not a valid date"
                    .format(v=date_val)
                )
                return
            elif isinstance(date_val, datetime.date):
                return date_val
            elif isinstance(date_val, datetime.datetime):
                return date_val.date()
            return
        except Exception as e:
            logger.debug(
                "convert_value: Error converting date value {} to a date; "
                "the current type is {}; error {}"\
                .format(date_val, type(date_val), str(e))
            )
            return

    def convert_bool(
            bool_val: Optional[Union[str, int, bool]]
        ) -> Optional[bool]:

        try:
            if not bool_val:
                return None
            elif isinstance(bool_val, bool):
                return bool_val
            elif isinstance(bool_val, int):
                return bool(bool_val)
            elif isinstance(bool_val, str):
                if re.match(true_str_pattern, bool_val):
                    return True
                elif re.match(false_str_pattern, bool_val):
                    return False
                else:
                    return None
            else:
                logger.debug(
                    "convert_value: unable to convert value {} to bool"
                    .format(bool_val)
                )
                return None
        except Exception as e:
            logger.debug(
                "convert_value: unable to convert value {} to bool; error: {}"
                .format(bool_val, str(e))
            )

    conversion_functions = {
        FieldType.Date:  convert_date,
        FieldType.Float: lambda v: round(float(v), 2),
        FieldType.Int:   int,
        FieldType.Str:   str,
        FieldType.Bool:  convert_bool
    }
    try:
        return conversion_functions[field_type](value)
    except Exception as e:
        logger.debug(
            'convert_value: Error converting value {} to data type {}; err:'
            .format(value, field_type, str(e))
        )
        return None


def format_value(*,
        field_type: FieldType,
        value: Optional[SqlDataType]=None,
        field_format: Optional[FieldFormat]=None
    ) -> SqlDataType:

    """Format a string value to a string appropriate for display to the user

    :param field_type:      The FieldType enum value representing the data type of the field
    :param value:           The value to format (can be None)
    :param field_format:    The FieldFormat enum value representing the format to use.
    :return:                The formatted value for display
    """
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
    data_type = inferred_data_types[field_format] if not field_type else field_type
    inferred_format = lambda fld_type: next(k for k, v in inferred_data_types.items() if v == field_type)
    format = inferred_format(field_type) if not field_format else field_format
    formatters = {
        FieldFormat.Accounting: lambda val: '{: ,.2f} '.format(round(val, 2)),
        FieldFormat.Bool:       lambda val: str(val),
        FieldFormat.Currency:   lambda val: '${: ,.2f} '.format(round(val, 2)),
        FieldFormat.Date:       lambda val: str(val)[:10],
        FieldFormat.DateTime:   lambda val: str(val),
        FieldFormat.Float:      lambda val: '{: ,.4f}'.format(round(val, 2)),
        FieldFormat.Int:        lambda val: '{: d}'.format(round(val, 0)),
        FieldFormat.Str:        lambda val: val
    }
    try:
        converted_val = convert_value(field_type=data_type, value=value)
        if converted_val is None:
            return None
        return formatters[format](converted_val)
    except Exception as e:
        logger.debug(
            'error formatting value,',
            'val:', value,
            'data_type:', data_type,
            'error msg:', str(e)
        )
        return value


@autorepr
class Field:
    """Instances of this class represent a column in a database table.

    :param name:            Field name on the database table
    :param dtype:           FieldType enum value representing data type
    :param display_name:    Name to display on the field's header
    :param field_format:    FieldFormat enum value indicating display format (e.g., decimal places)
    :param filters:         List of FilterConfig's representing list of filters to show on query designer
    :param editable:        Is this field editable?
    :param primary_key:     Is this field the primary key of the table?
    :param default_value:   When creating a new instance of the field, start with a default value for the field.
    """
    def __init__(self, *,
        name: str,
        dtype: FieldType,
        display_name: str,
        field_format: Optional[FieldFormat]=None,
        filters: Optional[List[FilterConfig]]=None,
        editable: bool=True,
        primary_key: bool=False,
        default_value: Any=None
    ) -> None:

        self.name = name
        self.dtype = dtype
        self.display_name = display_name
        self.field_format = field_format
        self.editable = editable
        self.primary_key = primary_key
        self.filters = filters
        self.default_value = default_value

    @static_property
    def schema(self) -> Column:
        """Map the field to a sqlalchemy Column"""
        type_map = {
            FieldType.Bool:  Boolean,
            FieldType.Date:  Date,
            FieldType.Float: Float,
            FieldType.Int:   Integer,
            FieldType.Str:   String
        }
        return Column(
            self.name,
            type_=type_map[self.dtype](),
            primary_key=self.primary_key
        )


@autorepr
class AdditiveField:
    """A field that represents an aggregate of a Fact.

    This field type is only used with Views over a Star.
    It mimics its base field except for the schema and editability.

    :param base_field_display_name: the display name of the Star field to aggregate.
                                    This can be a Field, SummaryField, or a
                                    CalculatedField.
    :param aggregate_display_name:  text to use for the output field's header
    :param aggregate_func:          instance of SqlAlchemy func enum to aggregate
                                    the field by
    """

    def __init__(self, *,
        base_field_display_name: FieldName,
        aggregate_display_name: FieldName,
        aggregate_func: func=func.sum
    ) -> None:

        self.logger = rotating_log('constellation.AdditiveField')

        self.base_field_display_name = base_field_display_name
        self.display_name = aggregate_display_name
        self.aggregate_func = aggregate_func

        # The star property is injected by the Star itself later.
        # It must be populated before this field can be used.
        self._star = None

        self.validate_config()

    @static_property
    def base_field(self) -> Field:
        """The base field on the star to aggregate

        The base field may, in fact, be a subquery rather than a simple field.
        """
        if not self.star:
            err_msg = 'AdditiveField {} must be assigned a star before use.' \
                      .format(self.display_name)
            self.logger.debug('base_field: {}'.format(err_msg))
            raise AttributeError(err_msg)
        try:
            return self.star.fields_by_display_name[self.base_field_display_name]
        except KeyError:
            self.logger.debug(
                'base_field: Error creating additive field {}; '
                'unable to find base field named {}'
                .format(self.display_name, self.base_field_display_name)
            )

    @static_property
    def dtype(self):
        """Mimic field property"""
        dtypes = {
            'count': FieldType.Int,
            'avg': FieldType.Float,
            'sum': FieldType.Float
        }
        try:
            return dtypes[self.sqa_func]
        except KeyError:
            self.logger.debug(
                'Unable to find data type of AdditiveField {}'
                .format(self.display_name)
            )
            return self.base_field.dtype

    @static_property
    def editable(self):
        """Mimic field property"""
        return False

    @static_property
    def field_format(self):
        """Mimic field property"""
        if self.dtype == FieldType.Int:
            return FieldFormat.Int
        return FieldFormat.Accounting

    @static_property
    def filter_operators(self):
        """Mimic field property"""
        return self.base_field.filter_operators

    @static_property
    def name(self):
        """Mimic field property"""
        return self.base_field.name

    @static_property
    def primary_key(self):
        """Mimic field property"""
        return False

    @static_property
    def schema(self):
        """SqlAlchemy representation of the AdditiveField"""
        try:
            return self.aggregate_func(self.base_field.schema)\
                   .cast(self.sqa_dtype).label(self.display_name)
        except Exception as e:
            self.logger.debug(
                'schema: Error creating aggregate field {}; error: {}'
                .format(self.display_name, str(e))
            )

    @static_property
    def sqa_dtype(self):
        """SqlAlchemy data type to cast to inside queries"""
        lkp = {
            'avg': Float(14, 2),
            'count': Integer,
            'sum': Float(14, 2)
        }
        try:
            return lkp[self.sqa_func]
        except KeyError:
            self.logger.debug(
                'sqa_dtype: Unable to find sqa_dtype for AdditiveField {} '
                'sqa_func {}'.format(self.display_name, self.sqa_func)
            )

    @static_property
    def sqa_func(self) -> str:
        """The name of the SqlAlchemy name associated with the field"""
        try:
            return self.aggregate_func._FunctionGenerator__names[0]
        except KeyError:
            self.logger.debug(
                'sqa_func: Error looking up sqa_func for AdditiveField {}'
                .format(self.display_name)
            )

    @static_property
    def star(self):
        """The Star this field belongs to"""
        if not self._star:
            err_msg = "The star for AdditiveField {} was not" \
                      "injected prior to calling the field." \
                      .format(self.display_name)
            self.logger.debug('star: {}'.format(err_msg))
            raise(AttributeError, err_msg)
        return self._star

    def validate_config(self):
        """Validate that the additive settings on config.cfg for this
        field meet certain criteria"""
        if self.sqa_func not in ['avg', 'count', 'sum']:
            self.logger.debug(
                'validate_config: The function {} for AdditiveField {} is not '
                'implemented.'.format(self.sqa_func, self.display_name)
            )


@autorepr
class Filter:
    """Holding tank for data needed to construct WHERE clause for field

    :param field:           base Field the filter is based on
    :param operator:        Operator enum value to apply to field
    :param default_value:   value to use in filter on initial load of app
    """
    def __init__(self, *,
        field: Field,
        operator: Operator,
        default_value: Optional[SqlDataType]=None
    ) -> None:

        self.field = field
        self.operator = operator
        self.default_value = default_value
        self._value = None  # type: Optional[SqlDataType]

    @static_property
    def display_name(self) -> str:
        suffix = self.operator.suffix
        return self.field.display_name + (" " + suffix if suffix else "")

    @property
    def filter(self) -> BinaryExpression:
        if self.value is None:
            return None
        elif self.value == '':
            return None
        else:
            fld = self.field.schema
            operator_mapping = {
                Operator.bool_is: lambda: self.field.schema == literal(self.value),
                Operator.bool_is_not: lambda: self.field.schema != literal(self.value),

                Operator.number_equals: lambda: fld == self.value,
                Operator.number_does_not_equal: lambda: fld != self.value,
                Operator.number_greater_than: lambda: fld > self.value,
                Operator.number_greater_than_or_equal_to: lambda: fld >= self.value,
                Operator.number_less_than: lambda: fld < self.value,
                Operator.number_less_than_or_equal_to: lambda: fld <= self.value,

                Operator.str_equals: lambda: fld == self.value,
                Operator.str_like: lambda: fld.contains(self.value),
                Operator.str_not_like: lambda: fld.notlike('%{}%'.format(self.value)),
                Operator.str_starts_with: lambda: fld.startswith(self.value),
                Operator.str_ends_with: lambda: fld.endswith(self.value),

                Operator.date_after: lambda: func.date(fld) > self.value,
                Operator.date_on_or_after: lambda: func.date(fld) >= self.value,
                Operator.date_before: lambda: func.date(fld) < self.value,
                Operator.date_on_or_before: lambda: func.date(fld) <= self.value,
                Operator.date_equals: lambda: func.date(fld) == self.value,
                Operator.date_does_not_equal: lambda: func.date(fld) != self.value
            }
            return operator_mapping[self.operator]()

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


@autorepr
class CalculatedField:
    """A field that represents the combination of one or more fields in a Star.

    It mimics its base field except for the schema and editability"""
    def __init__(self, *,
        formula: str,
        display_name: FieldName,
        show_on_fact_table: bool=True,
        filters: Optional[List[FilterConfig]]=None,
        default_value: Optional[Any]=None
    ) -> None:

        self.logger = rotating_log('constellation.CalculatedField')

        self.formula = formula
        self.display_name = display_name
        self.show_on_fact_table = show_on_fact_table
        self.default_value = default_value
        self._filters = filters

        # The star property is injected by the Star itself later.
        # It must be populated before this field can be used.
        self._star = None

        self.validate_config()

    @static_property
    def base_field_lkp(self):
        return self.star.base_field_schema_lkp

    @static_property
    def dtype(self):
        """Mimic field property"""
        return FieldType.Float

    @static_property
    def editable(self):
        """Mimic field property"""
        return False

    @static_property
    def filters(self) -> Optional[List[Filter]]:
        if self._filters:
            try:
                return [
                    Filter(
                        field=self,
                        operator=flt.operator,
                        default_value=flt.default_value
                    )
                    for flt in self._filters
                ]
            except Exception as e:
                self.logger.debug(
                    "filters: Could not create filters for calculated field {}"
                    "; error: {}".format(self.display_name, str(e))
                )
        return []

    @static_property
    def parsed_formula(self):
        """Parsed sql expression components

        :return: The pyparsing parser for a calculated field expression.
        """
        op = oneOf('+ - / *')
        lpar = Literal('(').suppress()
        rpar = Literal(')').suppress()
        field_or_num = Regex(r'\w+\.?\w*')
        field_name = Combine(
            pypOptional(Literal('-')) + field_or_num #Word(alphanums)
            + ZeroOrMore(oneOf(['_', ' ']) + Word(alphanums))
        )
        field = Literal('[').suppress() + field_name + Literal(']').suppress()
        expr = Forward()
        atom = field | Group(lpar + expr + rpar)
        expr << atom + ZeroOrMore(op + expr)
        return expr.parseString(self.formula).asList()

    @static_property
    def evaluate_expression(self) -> BinaryExpression:
        operator_lkp = {
            '-': lambda v1, v2: v1 - v2,
            '*': lambda v1, v2: v1 * v2,
            '/': lambda v1, v2: v1 / v2,
            '+': lambda v1, v2: v1 + v2
        }

        def evaluate_field(fld):
            if isinstance(fld, BinaryExpression):
                return fld
            try:
                return float(fld)
            except ValueError:
                try:
                    field = self.base_field_lkp[fld]
                    try:
                        return field.cast(Float(19, 2))
                    except:
                        return float(field)
                except KeyError:
                    return fld

        def resolve_branches(expr):
            fld1, op, fld2 = expr
            if isinstance(fld1, list):
                fld1 = resolve_branches(fld1)
            if isinstance(fld2, list):
                fld2 = resolve_branches(fld2)
            return operator_lkp[op](
                evaluate_field(fld1),
                evaluate_field(fld2)
            )
        return resolve_branches(self.parsed_formula)

    @static_property
    def field_format(self):
        """Mimic field property"""
        if self.dtype == FieldType.Int:
            return FieldFormat.Int
        return FieldFormat.Accounting

    @static_property
    def filter_operators(self):
        """Mimic field property"""
        return self.base_field.filter_operators

    @static_property
    def name(self):
        """Mimic field property"""
        return self.display_name

    @static_property
    def schema(self) -> BinaryExpression:
        try:
            return self.evaluate_expression.label(self.display_name)
        except Exception as e:
            self.logger.debug(
                'schema: Error creating schema for calculated field {}; '
                'error: {}'.format(self.display_name, str(e))
            )

    @static_property
    def primary_key(self):
        """Mimic field property"""
        return False

    @static_property
    def star(self):
        if not self._star:
            err_msg = "The star for CalculatedField {} was not" \
                      "injected prior to calling the field." \
                      .format(self.display_name)
            self.logger.debug('star: {}'.format(err_msg))
            raise(AttributeError, err_msg)
        return self._star

    def validate_config(self):
        if not self.formula:
            self.logger.debug(
                "validate_config: The formula for CalculatedField {} is blank"
                .format(self.display_name)
            )


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
            filters: Optional[List[FilterConfig]]=None
    ) -> None:

        super(SummaryField, self).__init__(
            name="_".join(display_fields),
            dtype=FieldType.Str,
            display_name=display_name,
            field_format=FieldFormat.Str,
            filters=filters,
            editable=False,
            primary_key=False
        )

        self.display_fields = display_fields
        self.display_name = display_name
        self.separator = separator


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
        refresh_on_update: bool=False
    ) -> None:

        self.logger = rotating_log('constellation.Table')

        self.table_name = table_name
        self.display_name = display_name
        self.fields = fields
        self.editable = editable
        self.display_rows = display_rows
        self.order_by = order_by
        self.show_on_load = show_on_load
        self.refresh_on_update = refresh_on_update

    def add_row(self, values: List[str]) -> Insert:
        """Statement to add a row to the table given a list of values"""

        # we want to use the primary key assigned by the db rather
        # than the one we auto-generated as a placeholder
        values_sans_pk = {
            fld.name: convert_value(value=values[i],
                                    field_type=self.fields[i].dtype)
            for i, fld in enumerate(self.fields)
            if not fld.primary_key
        }
        return self.schema.insert().values(values_sans_pk)

    def delete_row(self, id: int) -> Delete:
        """Create sql statement to delete a row, given a primary key"""
        return self.schema.delete().where(self.primary_key == id)

    def field(self, name: str) -> Field:
        """Look up a field based on it's name on the table."""
        try:
            return next(fld for fld in self.fields if fld.name == name)
        except StopIteration:
            self.logger.debug(
                'field: Could not find table field named {} on table {}'
                .format(name, self.table_name)
            )

    @static_property
    def filters(self) -> List[Filter]:
        return [
            Filter(field=fld,
                   operator=flt.operator,
                   default_value=flt.default_value)
            for fld in self.fields if fld.filters
            for flt in fld.filters
        ]

    def filter_by_display_name(self, display_name: str) -> Filter:
        try:
            return next(flt for flt in self.filters if flt.display_name == display_name)
        except StopIteration:
            self.logger.debug(
                'filter_by_display_name: The filter named {} could not be '
                'found.'.format(display_name)
            )

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
            self.logger.debug(
                'primary_key: Could not find the primary key for table {}'
                .format(self.table_name)
            )

    @static_property
    def primary_key_index(self) -> PrimaryKeyIndex:
        try:
            return PrimaryKeyIndex(
                next(i for i, c in enumerate(self.schema.columns)
                     if c.primary_key)
            )
        except StopIteration:
            self.logger.debug(
                'primary_key_index: could not find the primary key index for '
                'table {}'.format(self.table_name)
            )

    @static_property
    def schema(self) -> sqlaTable:
        """Map table to a sqlalchemy table schema"""
        try:
            cols = [fld.schema for fld in self.fields]
            return sqlaTable(self.table_name, md, *cols)
        except Exception as e:
            self.logger.debug(
                'schema: Error creating the schema for table {}; error: {}'
                .format(self.table_name, str(e))
            )

    def update_row(self, *,
        pk: PrimaryKeyIndex,
        values: List[SqlDataType]
    ) -> Update:

        """Statement to update a row on the table given the primary key value."""
        vals = {
            fld.name: convert_value(value=values[i],
                                    field_type=self.fields[i].dtype)
            for i, fld in enumerate(self.fields)
            if not fld.primary_key
        }
        return self.schema.update()\
                          .where(self.primary_key == pk)\
                          .values(vals)


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
            display_rows: int=10000,
            refresh_on_update: bool=False
    ) -> None:

        super(Dimension, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            editable=editable,
            show_on_load=show_on_load,
            display_rows=display_rows,
            order_by=order_by,
            refresh_on_update=refresh_on_update
        )

        self.summary_field = summary_field
        self.refresh_on_update = refresh_on_update

    @static_property
    def display_field_schemas(self) -> List[Column]:
        """Create the sqla schema to display for foreign key values
        on the one-side of the tables relationship with another."""

        def fld_schema(fld_name: FieldName):
            """If the dimension is associated with another dimension
            use the schema for the summary field of that dimension as
            a part of the currend dimensions summary field schema."""
            fld = self.field(fld_name)
            if isinstance(fld, ForeignKey):
                from star_schema.config import cfg
                try:
                    fk = next(
                        dim for dim in cfg.dimensions
                        if dim.table_name == fld.dimension
                    )
                    return fk.summary_field_schema.schema
                except StopIteration:
                    self.logger.error(
                        'display_field_schema: Could not find a '
                        'dimension for fk field {}.'.format(fld)
                    )
            return fld.schema

        return [
            fld_schema(n)
            for n in self.summary_field.display_fields
        ]

    @static_property
    def foreign_key_schema(self) -> Table:
        summary_field = reduce(
            lambda x, y: x + self.summary_field.separator + y,
            self.display_field_schemas
        ).label(self.summary_field.display_name)
        return select([self.primary_key, summary_field])

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
        filters_to_apply = (
            flt for flt
            in self.filters
            if flt._value != ''
                and flt._value is not None
        )
        for f in filters_to_apply:
            s = s.where(f.filter)
        if self.order_by_schema:
            for o in self.order_by_schema:
                s = s.order_by(o)
        return s.limit(max_rows)

    @static_property
    def summary_field_schema(self) -> Column:
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
            filters=None,  # filters for fks are created on the Star
            editable=True,
            primary_key=False
        )

        self.dimension = dimension
        self.foreign_key_field = foreign_key_field  # name of id field on dim

    @static_property
    def schema(self) -> Column:
        return Column(
            self.name,
            None,
            sqlaForeignKey("{t}.{f}".format(t=self.dimension,
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
            order_by: Optional[List[OrderBy]]=None,
            calculated_fields: Optional[List[CalculatedField]]=None,
            refresh_on_update: bool=False
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

        self.calculated_fields = calculated_fields
        self.refresh_on_update = refresh_on_update

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

        self.logger = rotating_log('constellation.Star')

        self.fact = fact
        self._dimensions = dimensions

    @static_property
    def calculated_fields(self):
        if self.fact.calculated_fields:
            for fld in self.fact.calculated_fields:
                fld._star = self
            return [fld for fld in self.fact.calculated_fields]
        return []

    @static_property
    def dimensions(self) -> Iterable[Dimension]:
        return [
            dim for dim in self._dimensions
            if dim.table_name in [
                fld.dimension
                for fld in self.fact.fields
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
        """Fields list"""
        return self.fact.fields + self.calculated_fields

    @static_property
    def base_field_schema_lkp(self) -> Dict[FieldName, Field]:
        """Base fields indexed by their display name"""
        dim_fields = {}
        for dim in self.dimensions:
            for fld in dim.fields:
                dim_fields[fld.display_name] = fld.schema
        fact_fields = {
            fld.display_name: fld.schema
            for fld in self.fact.fields
        }
        return ChainMap({}, fact_fields, dim_fields)

    @static_property
    def filters(self) -> List[Filter]:
        star_filters = []  # type: List
        for dim in self.dimensions:
            for flt in dim.summary_field.filters:
                fk_filter = Filter(
                    field=dim.summary_field_schema,
                    operator=flt.operator,
                    default_value=flt.default_value
                )
                star_filters.append(fk_filter)
        for f in (flt for flt in self.fact.filters):
            star_filters.append(f)
        for fld in self.calculated_fields:
            for flt in fld.filters:
                star_filters.append(flt)
        return sorted(star_filters)

    @static_property
    def fields_by_display_name(self) -> Dict[FieldName, Field]:
        return {
            fld.display_name: fld
            for fld in self.fields
        }

    @static_property
    def refresh_on_update(self):
        return self.fact.refresh_on_update

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
                fld = self.fields_by_display_name[order_by.field_name].schema
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
            calculated_fields = [
                fld.schema
                for fld in self.calculated_fields
            ]
            fact = self.fact.schema  # type: sqa.Table
            star = fact
            for dim in self.dimensions:
                star = star.outerjoin(dim.schema)
            if calculated_fields:
                fields = fact.columns + calculated_fields
            else:
                fields = fact.columns
            qry = select(fields).select_from(star)
            for f in [flt for flt in self.filters if flt.value]:
                qry = qry.where(f.filter)
            if self.order_by_schema:
                # noinspection PyTypeChecker
                for o in self.order_by_schema:
                    qry = qry.order_by(o)
            return qry
        except Exception as e:
            self.logger.debug(
                'star_query: Error composing star query: {}'
                .format(str(e))
            )


@autorepr
class View:
    """An aggregate view over a Star"""

    def __init__(self, *,
        view_display_name: str,
        fact_table_name: FactName,
        group_by_field_names: List[FieldName],
        additive_fields: Optional[List[AdditiveField]],
        order_by: Optional[List[OrderBy]] = None,
        show_on_load: bool=False
    ) -> None:

        self.logger = rotating_log('constellation.View')

        self.display_name = view_display_name  # type: str
        self._fact_table_name = fact_table_name  # type: str
        self._group_by_fields = group_by_field_names  # type: Optional[List[FieldName]]
        self._additive_fields = additive_fields  # type: Optional[List[AdditiveField]]
        self.primary_key_index = -1
        self.editable = False
        self.show_on_load = show_on_load
        self.order_by = order_by

    @static_property
    def additive_fields(self) -> List[AdditiveField]:
        if self._additive_fields:
            for fld in self._additive_fields:
                fld._star = self.star
            return self._additive_fields
        return []

    @static_property
    def star(self) -> Star:
        """Get a reference to the Star associated with the current Fact table"""
        from star_schema.config import cfg
        return cfg.star(fact_table=self._fact_table_name)

    def field_by_display_name(self, display_name: FieldName) -> Field:
        """Lookup a Field by it's display name."""
        try:
            return next(fld
                        for fld in self.fields
                        if fld.display_name == display_name)
        except KeyError:
            self.logger.debug(
                'field_by_display_name: Error looking up field_by_display_name; '
                'could not find a field named {} in the View {}'
                .format(display_name, self.display_name)
            )
        except Exception as e:
            self.logger.debug(
                'field_by_display_name: Error looking up field_by_display_name: '
                'err {}'.format(str(e))
            )

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
            self.star.fields_by_display_name[fld_name]
            for fld_name in self._group_by_fields
        ]

    @static_property
    def fields_schema(self):
        return [fld.schema for fld in self.group_by_fields] \
               + [fld.schema for fld in self.additive_fields]

    @static_property
    def fields(self):
        return self.group_by_fields + self.additive_fields

    @static_property
    def order_by_schema(self) -> List[Column]:
        """Return the order by fields for the View"""
        def lkp_sort_order(order_by: OrderBy):
            try:
                fld = self.field_by_display_name(order_by.field_name)
                if order_by.sort_order == SortOrder.Ascending:
                    return fld.schema.asc()
                return fld.schema.desc()
            except KeyError:
                self.logger.debug(
                    'view.py: Unable to look up sort order for View {}, '
                    'field {}.'.format(self.display_name, order_by.field_name)
                )

        if self.order_by:
            return [
                lkp_sort_order(o)
                for o in self.order_by
            ]
        return []

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
            if self.order_by_schema:
                for o in self.order_by_schema:
                    qry = qry.order_by(o)
            return qry.limit(self.star.display_rows)
        except Exception as e:
            self.logger.debug(
                'select: Error composing select statement for View {}; '
                'error {}'.format(self.display_name, str(e)))


class Constellation:
    """Collection of all the Stars in the application"""

    def __init__(self, *,
        app,
        dimensions: Optional[List[Dimension]],
        facts: List[Fact],
        views: List[View]
    ) -> None:

        self.logger = rotating_log('constellation.Constellation')

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

    @static_property
    def tables(self) -> List[Table]:
        return chain(self.facts, self.dimensions, self.views)

    @property
    def foreign_key_lookups(self) -> Dict[DimensionName, Select]:
        try:
            return {
                tbl.table_name: tbl.foreign_key_schema
                for tbl in self.dimensions
            }
        except Exception as e:
            self.logger.debug('foreign_key_lookups: error {}'
                              .format(str(e)))

    def foreign_keys(self, dim: DimensionName) -> Dict[ForeignKeyValue,
                                                       SqlDataType]:
        if self._foreign_keys.get(dim):
            return self._foreign_keys[dim]
        try:
            self.pull_foreign_keys(dim)
            fks = self._foreign_keys[dim]
            if 0 not in fks:
                self._foreign_keys[dim][0] = ""
                fks[0] = ""
            return fks
        except KeyError:
            self.logger.debug(
                'foreign_keys: Unable to find the Dimension {}; '
                .format(dim)
            )
        except Exception as e:
            self.logger.debug('foreign_keys: error {}; '.format(str(e)))

    def pull_foreign_keys(self, dim: DimensionName) -> None:
        try:
            select_statement = self.foreign_key_lookups[dim]  # type: Select
            from star_schema.db import fetch
            self._foreign_keys[dim] = ValueSortedDict({
                row[0]: str(row[1])
                for row in fetch(select_statement)
            })
        except Exception as e:
            self.logger.debug(
                'pull_foreign_keys: Error pulling foreign keys for dimension {}'
                .format(dim)
            )

    def star(self, fact_table: FactName) -> Star:
        """Return the specific Star system localized on a specific Fact table"""
        try:
            return self.stars[fact_table]
        except KeyError:
            self.logger.debug(
                'star: The fact table {} could not be found in the cfg global '
                'variable.'.format(fact_table)
            )

    def view(self, view_name: ViewName) -> View:
        """Return the specified View"""
        try:
            return next(view for view in self.views if view.display_name == view_name)
        except StopIteration:
            self.logger.debug(
                'view: A view with the display name {} could not be found in '
                'the cfg global variable.'.format(view_name)
            )

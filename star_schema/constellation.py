import datetime
import logging
from collections import ChainMap, namedtuple
from functools import lru_cache
from functools import partial
from itertools import chain, groupby
import locale
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
from typing import (
    Optional,
    List,
    Dict,
    Iterable,
    Any,
    Union,
    Callable,
    Generator, Iterator)

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

from star_schema import md
from star_schema.config import default_config, get_config, ConstellationConfig, \
    AppConfig, DimensionConfig, FactConfig, FieldConfig, AdditiveFieldConfig, \
    FilterConfig, CalculatedFieldConfig, SummaryFieldConfig, ForeignKeyConfig, \
    LookupTableConfig, ViewConfig
from star_schema.custom_types import (
    FactTableName,
    DimensionTableName,
    ForeignKeyValue,
    SqlDataType,
    ViewName,
    FieldType,
    FieldFormat,
    FieldName,
    Operator,
    SortOrder,
    ColumnIndex,
    PrimaryKeyIndex,
    OrderBy,
    Validator,
    TableName,
    LookupTableName,
    FieldDisplayName,
    DisplayField)
from star_schema.custom_types import PrimaryKeyValue
from star_schema.db import fetch
from star_schema.utilities import (
    static_property,
    autorepr
)

module_logger = logging.getLogger('app.' + __name__)

date_str_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}.*$")
true_str_pattern = re.compile(".*true.*", re.IGNORECASE)
false_str_pattern = re.compile(".*false.*", re.IGNORECASE)

locale.setlocale(locale.LC_ALL, '')


def convert_value(*,
        field_type: FieldType,
        value: Optional[SqlDataType]=None,
        validator: Optional[Validator]=None
    ) -> SqlDataType:
    """Convert a string value to a Python data type

    This conversion function is used to translate user input to a form that
    sqlalchemy can use.
    """
    def raise_err():
        logger = module_logger.getChild('convert_value')
        err_msg = "Unable to convert value {} to {}" \
                  .format(value, field_type)
        logger.debug(err_msg)
        raise ValueError(err_msg)

    default_values = {
        FieldType.Int:   0,
        FieldType.Str:   '',
        FieldType.Float: 0.0,
        FieldType.Bool:  False,
        FieldType.Date:  None
    }

    if value is None:
        return
    if not value:
        return default_values[field_type]
    if isinstance(value, float):
        if isnan(value) or isinf(value):
            raise_err()

    def convert_date(date_val: Optional[SqlDataType]) -> datetime.date:
        try:
            if isinstance(date_val, str):
                if date_str_pattern.match(date_val):
                    return datetime.datetime.strptime(date_val[:10], "%Y-%m-%d").date()
                else:
                    raise_err()
            elif isinstance(date_val, datetime.date):
                return date_val
            elif isinstance(date_val, datetime.datetime):
                return date_val.date()
            else:
                raise_err()
        except Exception as e:
            raise_err()

    def convert_bool(
            bool_val: Optional[SqlDataType]
        ) -> Optional[bool]:

        try:
            if isinstance(bool_val, bool):
                return bool_val
            elif isinstance(bool_val, int) or isinstance(bool_val, float):
                v = int(bool_val)
                if v in [0, 1]:
                    return bool(int(bool_val))
                else:
                    raise_err()
            elif isinstance(bool_val, str):
                if re.match(true_str_pattern, bool_val):
                    return True
                elif re.match(false_str_pattern, bool_val):
                    return False
                else:
                    raise_err()
            else:
                raise_err()
        except:
            raise_err()

    def convert_float(float_val: Optional[SqlDataType]) -> bool:
        try:
            return round(float(float_val), 2)
        except:
            raise_err()


    conversion_functions = {
        FieldType.Date:  convert_date,
        FieldType.Float: convert_float,
        FieldType.Int:   int,
        FieldType.Str:   str,
        FieldType.Bool:  convert_bool
    }
    try:
        cval = conversion_functions[field_type](value)
        if validator:
            valid = validator(cval)
            if valid[0]:
                raise ValueError(valid[1])
        return cval
    except:
        raise_err()


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
        FieldFormat.Accounting: lambda val: locale.currency(round(val, 2), symbol=False, grouping=True),
        FieldFormat.Bool:       lambda val: str(val),
        FieldFormat.Currency:   lambda val: locale.currency(round(val, 2), international=True, grouping=True),
        FieldFormat.Date:       lambda val: str(val)[:10],
        FieldFormat.DateTime:   lambda val: str(val),
        FieldFormat.Float:      lambda val: locale.currency(round(val, 4), symbol=False, grouping=True),
        FieldFormat.Int:        lambda val: locale.format_string('%d', round(val, 0), grouping=True),
        FieldFormat.Str:        lambda val: val
    }
    try:
        converted_val = convert_value(field_type=data_type, value=value)
        if converted_val is None:
            return
        return formatters[format](converted_val)
    except Exception as e:
        logger = module_logger.getChild('format_value')
        err_msg = "Error formatting value {}, type {}".format(value, data_type)
        logger.debug(err_msg)
        raise ValueError(err_msg)


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
    :param visible          Display the field on the views?
    :param validator        Function to run when upddating adding rows
                            If the function returns False for the value
                            of the field then the transaction will be rejected.
    """
    def __init__(self, config: Union[FieldConfig, SummaryFieldConfig]) -> None:
        self.config = config
        self.name = config.name
        self.dtype = config.dtype
        self.display_name = config.display_name
        self.field_format = config.field_format
        self.editable = config.editable
        self.primary_key = config.primary_key
        self.default_value = config.default_value
        self.visible = config.visible
        self.validator = config.validator

    @property
    def filters(self):
        return [
            Filter(flt_cfg)
            for flt_cfg in self.config.filters
        ]

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


# @autorepr
# class ManyToManyField:
#     """This class merely stores the configuration for the nexus of many to many fields
#     """
#     def __init__(self, *,
#         lookup_table_name: TableName,
#         display_name: FieldName,
#         field_separator: str
#     ) -> None:
#
#         self.lookup_table_name = lookup_table_name
#         self.display_name = display_name
#         self.field_separator = field_separator
#
#         # mimicked field attributes
#         self.name = display_name
#         self.dtype = FieldType.Str
#         self.field_format = FieldFormat.Str
#         self.editable = True
#         self.primary_key = False
#         self.default_value = ''
#         self.visible = True
#         self.filters = []
#
#     @static_property
#     def schema(self):
#         """A dummy placeholder; the values are replaced during processing"""
#         # return Column(
#         #     'None',
#         #     type_=String,
#         #     is_literal=True
#         # )
#
#         return literal_column("'None'").label(self.display_name)


@autorepr
class AdditiveField:
    """A field that represents an aggregate of a Fact.

    This field type is only used with Views over a Star.
    It mimics its base field except for the schema and editability.
    """
    def __init__(self, config: AdditiveFieldConfig, star) -> None:
        self.logger = module_logger.getChild('AdditiveField')
        self.config = config
        self.base_field_display_name = config.base_field_display_name
        self.display_name = config.aggregate_display_name
        self.aggregate_func = config.aggregate_func
        self.visible = config.visible
        self.star = star

    @static_property
    def base_field(self) -> Field:
        """The base field on the star to aggregate

        The base field may, in fact, be a subquery rather than a simple field.
        """
        if not self.star:
            err_msg = 'base_field: {} must be assigned a star before use.' \
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


@autorepr
class Filter:
    """Holding tank for data needed to construct WHERE clause for field

    :param field:           base Field the filter is based on
    :param operator:        Operator enum value to apply to field
    :param default_value:   value to use in filter on initial load of app
    """
    def __init__(self, config: FilterConfig) -> None:
        self.config = config
        self.field = Field(config.field)
        self.operator = config.operator
        self.default_value = config.default_value
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

    It mimics its base field except for the schema and editability

    :param formula:             String formula using field display names
    :param display_name:        Name of field to display on header
    :param show_on_fact_table:  Show this field on the main fact table view
                                Fields that are merely used as intermediates may
                                be not be useful to show.
    :param filters:             Filters to display on QueryDesigner for this field
    :param default_value:       Default value to display when a new record is added
    :param visible              Display the field on the views?
    """
    def __init__(self, config: CalculatedFieldConfig) -> None:
        self.logger = module_logger.getChild('CalculatedField')
        self.config = config
        self.formula = config.formula
        self.display_name = config.display_name
        self.show_on_fact_table = config.show_on_fact_table
        self.default_value = config.default_value
        self.visible = config.visible

        # The star property is injected by the Star itself later.
        # It must be populated before this field can be used.
        self._star = None

    @static_property
    def base_field_lkp(self):
        return self.star.base_field_schema_lkp

    @static_property
    def dtype(self):
        """Mimic field property"""
        return FieldType.Float

    @static_property
    def filters(self):
        return [
            Filter(flt_cfg)
            for flt_cfg in self.config.filters
        ]

    @static_property
    def editable(self):
        """Mimic field property"""
        return False

    # @static_property
    # def filters(self) -> Optional[List[Filter]]:
    #     if self._filters:
    #         try:
    #             return [
    #                 Filter(
    #                     field=self,
    #                     operator=flt.operator,
    #                     default_value=flt.default_value
    #                 )
    #                 for flt in self._filters
    #             ]
    #         except Exception as e:
    #             self.logger.debug(
    #                 "filters: Could not create filters for calculated field {}"
    #                 "; error: {}".format(self.display_name, str(e))
    #             )
    #     return []

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


@autorepr
class SummaryField(Field):
    """Concatenate multiple fields

    This field type is used for display on associated fact tables in lieu of
    their integer primary key.
    """
    def __init__(self, config: SummaryFieldConfig, schema) -> None:
        super(SummaryField, self).__init__(config)
        self.config = config
        self.display_fields = config.display_fields
        self.display_name = config.display_name
        self.separator = config.separator
        self.visible = config.visible
        self.schema = schema


@autorepr
class Table:
    """A container to store fields

    This class is meant to be subclassed by Dimension and Fact table classes.
    """
    def __init__(self, config: Union[DimensionConfig, FactConfig]) -> None:
        self.logger = module_logger.getChild('Table')
        self.config = config
        self.table_name = config.table_name
        self.display_name = config.display_name
        self.editable = config.editable
        self.display_rows = config.display_rows
        self.order_by = config.order_by
        self.show_on_load = config.show_on_load
        self.refresh_on_update = config.refresh_on_update

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

    @lru_cache(maxsize=10)
    def field(self, name: FieldName) -> Field:
        """Look up a field based on it's name on the table."""
        try:
            return next(fld for fld in self.fields if fld.name == name)
        except StopIteration:
            self.logger.debug(
                'field: Could not find table field named {} on table {}'
                .format(name, self.table_name)
            )

    @static_property
    def fields(self):
        return [
            Field(fld_cfg)
            for fld_cfg in self.config.fields
        ] + [
            ForeignKey(fk_cfg)
            for fk_cfg in self.config.foreign_keys
        ]

    @static_property
    def filters(self) -> List[Filter]:
        return [
            flt
            for fld in self.fields if fld.filters
            for flt in fld.filters
        ]

    def filter_by_display_name(self, display_name: str) -> Filter:
        try:
            return next(
                flt for flt in self.filters
                if flt.display_name == display_name
            )
        except StopIteration:
            self.logger.debug(
                'filter_by_display_name: The filter named {} could not be '
                'found.'.format(display_name)
            )

    @static_property
    def foreign_keys(self) -> List[Field]:
        return [
            ForeignKey(fk_cfg)
            for fk_cfg in self.config.foreign_keys
        ]

    @static_property
    def primary_key(self) -> Field:
        try:
            return next(
                c for c in self.schema.columns
                if c.primary_key is True
            )
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
        except (StopIteration, AttributeError):
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
            config: Union[DimensionConfig, LookupTableConfig],
            dim_lkp_fn: Callable[[DimensionTableName], Table]
        ) -> None:

        self.logger = module_logger.getChild('Dimension')
        self.config = config
        # self.summary_field = SummaryField(config.summary_field)
        self.refresh_on_update = config.refresh_on_update
        self.dim_lkp_fn = dim_lkp_fn
        super(Dimension, self).__init__(config)

    @static_property
    def summary_field(self) -> SummaryField:
        """Create the sqla schema to display for foreign key values
        on the one-side of the tables relationship with another."""

        def fld_schema(fld_name: FieldName):
            """If the dimension is associated with another dimension
            use the schema for the summary field of that dimension as
            a part of the currend dimensions summary field schema."""
            fld = self.field(fld_name)
            if isinstance(fld, ForeignKey):
                try:
                    fk = self.dim_lkp_fn(fld.dimension)
                    if fk:
                        return fk.summary_field.schema
                    else:
                        raise KeyError(
                            'Could not find the foreign key for field {}; on'
                            'dimension {}'
                            .format(fld_name, fld.dimension)
                        )
                except Exception as e:
                    self.logger.error(
                        'summary_field.fld_schema: Could not find a '
                        'dimension for fk field {}; table_name: {}; '
                        'error {}'
                        .format(fld.display_name, fld.dimension, str(e))
                    )
            return fld.schema
        try:
            display_schema = [
                fld_schema(n)
                for n in self.config.summary_field.display_fields
            ]
            schema = reduce(
                lambda x, y: x + self.config.summary_field.separator + y,
                display_schema
            ).label(self.config.summary_field.display_name)
            return SummaryField(config=self.config.summary_field, schema=schema)
        except Exception as e:
            self.logger.error(
                'display_field_schemas: Could not determine schema; error {}'
                .format(str(e))
            )
            raise

    @static_property
    def foreign_key_schema(self) -> Table:
        return select([self.primary_key, self.summary_field.schema])

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


@autorepr
class ForeignKey(Field):
    def __init__(self, config: ForeignKeyConfig) -> None:
        super(ForeignKey, self).__init__(config)
        self.config = config
        self.dimension = config.dimension
        self.foreign_key_field = config.foreign_key_field  # name of id field on dim
        self.visible = config.visible

    @static_property
    def schema(self) -> Column:
        return Column(
            self.name,
            Integer,
            sqlaForeignKey("{t}.{f}".format(t=self.dimension,
                                            f=self.foreign_key_field))
        )


@autorepr
class LookupTable(Dimension):
    """Lookup table specifications

    We don't specify the maximum display or export rows since lookup tables
    should be (and in this case *must* have a low row count, and the user must
    be able to see the entire dimension to edit any foreign keys that may show
    up on the associated Fact table.
    """
    def __init__(self, *,
            config: LookupTableConfig,
            dim_lkp_fn: Callable[[DimensionTableName], Dimension]
        ) -> None:

        self.logger = module_logger.getChild('Dimension')
        self.config = config
        super(LookupTable, self).__init__(config=config, dim_lkp_fn=dim_lkp_fn)

        self.id_field = Field(config.id_field)
        self.proximal_fk = ForeignKey(config.proximal_fk)
        self.distal_fk = ForeignKey(config.distal_fk)
        self.fields = [
            self.id_field,
            self.proximal_fk,
            self.distal_fk
        ]
        # self.summary_field = SummaryField(config.summary_field)
        self.refresh_on_update = config.refresh_on_update

    # @static_property
    # def extra_fields(self):
    #     return [
    #         Field(ext_cfg)
    #         for ext_cfg in self.config.extra_fields
    #     ]

    @static_property
    def lookup_schema(self) -> Table:
        return select([self.proximal_fk.schema, self.distal_fk.schema]) \
               .select_from(self.schema)


@autorepr
class Fact(Table):
    """Fact table specification

    Fact tables are generally long, but not wide.  They primarily contain data
    that is aggregated, and references to dimension tables for contextual data.
    They may also contain 'junk' dimensions, which are simply dimensions that
    don't warrant a separate table to store them.
    """

    def __init__(self, config: FactConfig) -> None:
        self.config = config
        super(Fact, self).__init__(config)
        self.refresh_on_update = config.refresh_on_update

    @static_property
    def calculated_fields(self):
        return [
            CalculatedField(calc_cfg)
            for calc_cfg in self.config.calculated_fields
        ]

    @property
    def dimensions(self) -> List[DimensionTableName]:
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

        self.logger = module_logger.getChild('Star')

        self.fact = fact  # type: Fact
        self._dimensions = dimensions  # type: List[Dimension]

    @static_property
    def calculated_fields(self):
        if self.fact.calculated_fields:
            for fld in self.fact.calculated_fields:
                fld._star = self
            return [fld for fld in self.fact.calculated_fields]
        return []

    @static_property
    def dimensions(self) -> List[Dimension]:
        return [
            dim for dim in self._dimensions
            if dim.table_name in [
                fld.dimension
                for fld in self.fact.fields
                if isinstance(fld, ForeignKey)
            ]
        ]

    @static_property
    def display_name(self):
        return self.fact.display_name

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
                fk_filter = Filter(flt.config)
                #     field=dim.summary_field_schema,
                #     operator=flt.operator,
                #     default_value=flt.default_value
                # )
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
            str(dim.summary_field.display_name): dim.summary_field.schema
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
            self.logger.error(
                'star_query: Error composing star query: {}'
                .format(str(e))
            )
            raise


@autorepr
class View:
    """An aggregate view over a Star
    """
    def __init__(self, config: ViewConfig, star: Star) -> None:
        self.logger = module_logger.getChild('View')
        self.config = config
        self.display_name = config.view_display_name  # type: str
        self.fact_table_name = config.fact_table_name  # type: str
        self.table_name = self.fact_table_name  # TODO
        self.primary_key_index = -1
        self.editable = False
        self.show_on_load = config.show_on_load
        self.order_by = config.order_by
        self.star = star

    @static_property
    def additive_fields(self) -> List[AdditiveField]:
        if self.config.additive_fields:
            return [
                AdditiveField(config=add_cfg, star=self.star)
                for add_cfg in self.config.additive_fields
            ]
        return []

    def field_by_display_name(self, display_name: FieldName) -> Field:
        """Lookup a Field by it's display name."""
        try:
            return next(
                fld for fld in self.fields
                if fld.display_name == display_name
            )
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
            for fld_name in self.config.group_by_field_names
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


@autorepr
class DisplayPackage:
    """This class stores the specifications to send to the view

    :param table:
        Base table, star, or view that the queries are created from.
    :param display_name:
        The name to show on the tab for the table.
    :param foriegn_keys
        The foreign key dictionary associated with a field, indexed by display name.
    :param calculated_field_fn:
        If the table has any calculated fields, they are calculated on
        editable tables during post-processing on the Model.
    """
    def __init__(self, *,
        app,
        table: Table,
        display_base: Union[Table, Star, View],
        primary_key_index: PrimaryKeyIndex,
        field_order: Dict[ColumnIndex, FieldDisplayName],
        refresh_foreign_keys: Callable[[DimensionTableName], None],
        foreign_keys:
            Optional[
                Dict[FieldDisplayName,
                     Dict[int, str]
                ]
            ]=None,
        calculated_field_fns:
            Optional[
                Dict[FieldName,
                     Callable[[List[SqlDataType]],
                              Callable]
                ]
            ]=None,
        lookup_fields:
            Optional[
                Dict[FieldDisplayName, Dict[PrimaryKeyValue, str]]
            ]=None
        ) -> None:

        self.app = app
        self.table = table
        self.display_base = display_base
        self.foreign_keys = foreign_keys
        self.calculated_field_fns = calculated_field_fns
        self.field_order = field_order
        self.lookup_fields = lookup_fields
        self.primary_key_index = primary_key_index
        self.refresh_foreign_keys = refresh_foreign_keys


@autorepr
class App:
    """Configuration settings for application-wide settings"""
    def __init__(self, config: AppConfig) -> None:
        self.color_scheme = config.color_scheme
        self.db_path = config.db_path
        self.display_name = config.display_name


def sort_fields(
        fields: List[Field],
        default_field_order: Optional[List[FieldDisplayName]]=None,
        many_to_many_fk_names: Optional[Iterable[FieldDisplayName]]=None
    ) -> List[DisplayField]:

    LookupField = namedtuple(
        'LookupField',
        'name display_name'
    )
    field_partitions = {
        AdditiveField:   [],
        CalculatedField: [],
        Field:           [],
        ForeignKey:      [],
        LookupField:     []
    }
    for i, fld in enumerate(fields):
        fo = DisplayField(display_index=None,
            original_index=i,
            name=fld.name,
            display_name=fld.display_name,
            field_type=type(fld),
            dtype=fld.dtype,
            field_format=fld.field_format,
            editable=fld.editable,
            visible=fld.visible,
            dimension=fld.__dict__.get('dimension')
        )
        field_partitions[type(fld)].append(fo)
    for i, k in enumerate(many_to_many_fk_names,
            start=len(fields)):
        fo = DisplayField(display_index=None,
            original_index=i,
            name=k,
            display_name=k,
            field_type=LookupField,
            dtype=FieldType.Str,
            field_format=FieldFormat.Str,
            editable=False,
            visible=True,
            dimension=None
        )
        field_partitions[LookupField].append(fo)
    return [
        DisplayField(display_index=i,
            original_index=fld.original_index,
            name=fld.name,
            display_name=fld.display_name,
            field_type=fld.field_type,
            dtype=fld.dtype,
            field_format=fld.field_format,
            editable=fld.editable,
            visible=fld.visible,
            dimension=fld.dimension
        )
        for i, fld
        in enumerate(chain(field_partitions[Field],
            field_partitions[ForeignKey],
            field_partitions[LookupField],
            field_partitions[AdditiveField],
            field_partitions[CalculatedField]))
    ]


class Constellation:
    """Collection of all the Stars in the application"""

    def __init__(self, config: ConstellationConfig) -> None:
        self.logger = module_logger.getChild('Constellation')
        self.config = config
        self._foreign_keys = {
            tbl.table_name: {}
            for tbl in self.dimensions
        }  # type: Dict[TableName, Dict[PrimaryKeyValue, str]]

        # flag all dimensions as to_load initially
        self.dims_to_refresh = set(chain(
            [tbl.table_name for tbl in self.dimensions],
            [tbl.table_name for tbl in self.lookup_tables]
        ))

    @static_property
    def app(self):
        return App(self.config.app)

    @lru_cache(maxsize=10)
    def dimension(self, dim_name: DimensionTableName) -> Dimension:
        try:
            return next(d for d in self.dimensions
                        if d.table_name == dim_name)
        except StopIteration:
            self.logger.debug(
                'dimension: Unable to find a dimension named {}'
                .format(dim_name)
            )

    @static_property
    def dimensions(self) -> Iterator[Dimension]:
        return [
            Dimension(config=dim_cfg, dim_lkp_fn=self.dimension)
            for dim_cfg in self.config.dimensions
        ]

    @lru_cache(maxsize=10)
    def fact(self, fact_name: FactTableName) -> Dimension:
        try:
            return next(
                f for f in self.facts
                if f.table_name == fact_name
            )
        except StopIteration:
            self.logger.debug(
                'dimension: Unable to find a dimension named {}'
                .format(fact_name)
            )

    @static_property
    def facts(self) -> Iterator[Fact]:
        return [
            Fact(fact_cfg)
            for fact_cfg in self.config.facts
        ]

    @lru_cache(maxsize=10)
    def lookup_table(self, lkp_tbl_name: LookupTableName) -> LookupTable:
        try:
            return next(lkp for lkp in self.lookup_tables
                        if lkp.table_name == lkp_tbl_name)
        except StopIteration:
            self.logger.debug(
                'lookup_table: Unable to find a LookupTable named {}'
                .format(lkp_tbl_name)
            )

    @static_property
    def lookup_tables(self) -> List[LookupTable]:
        return [
            LookupTable(config=lkp_cfg, dim_lkp_fn=self.dimension)
            for lkp_cfg in self.config.lookup_tables
        ]

    @static_property
    def tables(self) -> List[Table]:
        return list(chain(
            self.facts,
            self.dimensions,
            self.lookup_tables,
            self.views
        ))

    @static_property
    def display_packages(self):
        """Create a package to send to the view for display"""
        packages = []
        for tbl in self.tables:
            try:
                bases = {
                    Fact: lambda: self.star(tbl.table_name),
                    View: lambda: self.view(tbl.display_name),
                    Dimension: lambda: tbl,
                    LookupTable: lambda: tbl
                }
                display_base = bases[type(tbl)]()
                if display_base:
                    one_to_many_fks = {}  # type: Dict[FieldDisplayName, Dict[PrimaryKeyValue, str]]
                    for fld in display_base.fields:
                        if isinstance(fld, ForeignKey):
                            one_to_many_fks[fld.display_name] = partial(self.foreign_keys, fld.dimension)
                    many_to_many_fks = {
                        lkp.distal_fk.display_name: partial(self.foreign_keys, lkp.table_name)
                        for lkp in self.lookup_tables
                        if lkp.proximal_fk.dimension == tbl.table_name
                    }  # type: Dict[FieldDisplayName,Dict[PrimaryKeyValue, str]]
                    field_order = sort_fields(
                        fields=display_base.fields,
                        default_field_order=None,
                        many_to_many_fk_names=many_to_many_fks.keys()
                    )
                    # noinspection PyTypeChecker
                    display_pkg = DisplayPackage(
                        app=self.config.app,
                        table=tbl,
                        display_base=display_base,
                        primary_key_index=0,
                        foreign_keys=one_to_many_fks,
                        calculated_field_fns=None,
                        lookup_fields=many_to_many_fks,
                        field_order=field_order,
                        refresh_foreign_keys=partial(self.refresh_foreign_keys)
                    )
                    packages.append(display_pkg)
                else:
                    err_msg = 'display_packages: Unable to find display base ' \
                              'for table {}'.format(tbl)
                    self.logger.error(err_msg)
                    raise AttributeError(err_msg)
            except Exception as e:
                self.logger.error(
                    'display_packages: Unable to create display package for '
                    'table {}; error {}'.format(tbl.table_name, str(e))
                )
                raise
        return packages

    def table(self, table_name: TableName):
        try:
            return next(
                tbl for tbl in self.tables
                if tbl.table_name == table_name
            )
        except StopIteration:
            self.logger.debug(
                'table: Unable to find a table named {}'
                .format(table_name)
            )

    @static_property
    def views(self):
        try:
            return [
                View(config=view_cfg, star=self.star(view_cfg.fact_table_name))
                for view_cfg in self.config.views
            ]
        except Exception as e:
            self.logger.error(
                "views: Error creating views; error: {}"
                .format(str(e))
            )
            raise

    def pull_one_to_many_fks(self, table_name: TableName) -> None:
        try:
            dim = self.dimension(table_name)
            select_statement = dim.foreign_key_schema
            self._foreign_keys[table_name] = ValueSortedDict({
                row[0]: str(row[1])
                for row in fetch(qry=select_statement,
                                 con_str=self.config.app.db_path)
            })
        except StopIteration:
            self.logger.error(
                'pull_one_to_many_fks: Could not find a dimension named {}'
                .format(table_name)
            )
            raise
        except Exception as e:
            self.logger.error(
                'pull_one_to_many_fks: Error pulling foreign keys for '
                'dimension {}; error: {}'.format(table_name, str(e))
            )
            raise

    def pull_many_to_many_fks(self, lkp_tbl_name: LookupTableName) -> None:
        if not self._foreign_keys.get(lkp_tbl_name):
            try:
                lkp_tbl = self.lookup_table(lkp_tbl_name)
                distal_dim_name = lkp_tbl.distal_fk.dimension
                distal_dim = self.dimension(distal_dim_name)

                if distal_dim:
                    rows = fetch(qry=lkp_tbl.lookup_schema,
                                 con_str=self.config.app.db_path)
                    distal_fks = {
                        row[0]: [r[1] for r in row[1]]
                        for row in groupby(rows, lambda x: x[0])
                    }
                    if not self._foreign_keys[distal_dim_name]:
                        self.pull_one_to_many_fks(distal_dim_name)
                    fks = self._foreign_keys[distal_dim_name]
                    if fks and distal_fks:
                        self._foreign_keys[lkp_tbl_name] = {
                            k: '; '.join(fks.get(val) for val in v)
                            for k, v in distal_fks.items()
                        }
            except Exception as e:
                self.logger.error(
                    'pull_many_to_many_fks: Unable to pull many-to-many '
                    'foreign-keys for lookup table {}; error {}'
                    .format(lkp_tbl_name, str(e))
                )
                raise

    def refresh_foreign_keys(self, dim: Union[DimensionTableName, LookupTableName]) -> None:
        self.dims_to_refresh.add(dim)

    def foreign_keys(self,
            tbl: Union[DimensionTableName, LookupTableName]
        ) -> Dict[ForeignKeyValue, SqlDataType]:

        if tbl in self.dims_to_refresh:
            self.dims_to_refresh.remove(tbl)
            try:
                if tbl in (lkp.table_name for lkp in self.lookup_tables):
                    self.pull_many_to_many_fks(tbl)
                else:
                    self.pull_one_to_many_fks(tbl)
                if 0 not in self._foreign_keys[tbl]:
                    self._foreign_keys[tbl][0] = ""
                print(self._foreign_keys[tbl])
                return self._foreign_keys[tbl]
            except KeyError:
                self.logger.debug(
                    'foreign_keys: Unable to find the Dimension {}; '
                    .format(tbl)
                )
            except Exception as e:
                self.logger.debug(
                    'foreign_keys: dim: {}; error {}'
                    .format(tbl, str(e))
                )
        else:
            return self._foreign_keys[tbl]

    @lru_cache(maxsize=10)
    def star(self, fact_table: FactTableName) -> Star:
        """Return the specific Star system localized on a specific Fact table"""
        try:
            return self.stars[fact_table]
        except KeyError:
            self.logger.debug(
                'star: The fact table {} could not be found'
                .format(fact_table)
            )
            raise

    @static_property
    def stars(self) -> Dict[FactTableName, Star]:
        return {
            fact.table_name: Star(fact=fact, dimensions=self.dimensions)
            for fact in self.facts
        }

    def view(self, view_name: ViewName) -> View:
        """Return the specified View"""
        try:
            return next(
                view for view in self.views
                if view.display_name == view_name
            )
        except StopIteration:
            self.logger.debug(
                'view: A view with the display name {} could not be found.'
                .format(view_name)
            )


@lru_cache(maxsize=5)
def get_constellation(json_path: Optional[str]=None):
    if json_path:
        return Constellation(get_config(json_path))
    else:
        return Constellation(default_config)

import datetime
import locale
import logging
import re
import weakref
from collections import ChainMap, defaultdict
from functools import lru_cache
from functools import partial
from functools import reduce
from itertools import chain, groupby
from math import isinf, isnan

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
from sortedcollections import ValueSortedDict
from sortedcontainers import SortedDict
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    Float,
    Integer,
    select,
    String
)
from sqlalchemy import ForeignKey as sqlaForeignKey
from sqlalchemy import Table as sqlaTable
from sqlalchemy import func
from sqlalchemy.sql import (
    Delete,
    Insert,
    Select,
    Update
)
from sqlalchemy.sql.elements import BinaryExpression, literal, UnaryExpression
from typing import Callable
from typing import (
    Optional,
    List,
    Dict,
    Union,
    Tuple
)

from star_schema import md
from star_schema.config import (
    default_config,
    get_config,
    ConstellationConfig,
    AppConfig,
    DimensionConfig,
    FactConfig,
    FieldConfig,
    AdditiveFieldConfig,
    FilterConfig,
    CalculatedFieldConfig,
    SummaryFieldConfig,
    ForeignKeyConfig,
    LookupTableConfig,
    ViewConfig
)
from star_schema.custom_types import (
    FactTableName,
    DimensionTableName,
    SqlDataType,
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
    TableDisplayName,
    FieldDisplayName
)
from star_schema.custom_types import PrimaryKeyValue
from star_schema.db import Transaction, fetch
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
    ) -> Optional[SqlDataType]:
    """Convert a string value to a Python data type

    This conversion function is used to translate user input to a form that
    SqlAlchemy can use.
    """
    def raise_err():
        logger = module_logger.getChild('convert_value')
        err_msg = "Unable to convert value {} to {}" \
                  .format(value, field_type)
        logger.debug(err_msg)
        raise ValueError(err_msg)

    if value is None or value == '':
        return

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

    def convert_int(int_val: Optional[SqlDataType]) -> int:
        try:
            if isinstance(int_val, str):
                return locale.atoi(int_val)
            else:
                return int(round(int_val, 0))
        except:
            raise_err()

    def convert_float(float_val: Optional[SqlDataType]) -> float:
        try:
            if isinstance(float_val, str):
                return locale.atof(float_val)
            else:
                return round(float(float_val), 2)
        except:
            raise_err()

    conversion_functions = {
        FieldType.Date:  convert_date,
        FieldType.Float: convert_float,
        FieldType.Int:   convert_int,
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

    :param field_type:      The FieldType enum value representing the data type
                            of the field
    :param value:           The value to format (can be None)
    :param field_format:    The FieldFormat enum value representing the format
                            to use.
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
        FieldFormat.Str:        FieldType.Str,
        FieldFormat.Memo:       FieldType.Str
    }
    data_type = inferred_data_types[field_format] if not field_type else field_type
    inferred_format = lambda fld_type: next(
        k for k, v in inferred_data_types.items() if v == field_type)
    format = inferred_format(field_type) if not field_format else field_format
    formatters = {
        FieldFormat.Accounting:
            lambda val: locale.currency(round(val, 2), symbol=False, grouping=True),
        FieldFormat.Bool:
            lambda val: str(val),
        FieldFormat.Currency:
            lambda val: locale.currency(round(val, 2), international=True, grouping=True),
        FieldFormat.Date:
            lambda val: str(val)[:10],
        FieldFormat.DateTime:
            lambda val: str(val),
        FieldFormat.Float:
            lambda val: locale.currency(round(val, 4), symbol=False, grouping=True),
        FieldFormat.Int:
            lambda val: locale.format_string('%d', round(val, 0), grouping=True),
        FieldFormat.Str:
            lambda val: val,
        FieldFormat.Memo:
            lambda val: val
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


@autorepr(str_attrs=['display_name', 'dtype'], repr_attrs=['config'])
class Field:
    """Instances of this class represent a column in a database table.
    """
    def __init__(self, config: Union[FieldConfig, SummaryFieldConfig], table) -> None:
        self.config = config
        self.table_name = config.table_name
        self.name = config.name
        self.dtype = config.dtype
        self.display_name = config.display_name
        self.field_format = config.field_format
        self.editable = config.editable
        self.primary_key = config.primary_key
        self.default_value = config.default_value
        self.visible = config.visible
        self.validator = config.validator
        self.table = table

    @property
    def filters(self) -> List['Filter']:
        return [
            Filter(flt_cfg, field=self)
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


@autorepr
class AdditiveField:
    """A field that represents an aggregate of a Fact.

    This field type is only used with Views over a Star.
    It mimics its base field except for the schema and editability.
    """
    logger = module_logger.getChild('AdditiveField')

    def __init__(self, config: AdditiveFieldConfig, parent) -> None:
        self.config = config
        self.base_field_display_name = config.base_field_display_name
        self.display_name = config.aggregate_display_name
        self.aggregate_func = config.aggregate_func
        self.visible = config.visible
        self.parent = parent

    @static_property
    def base_field(self) -> Field:
        """The base field on the star to aggregate

        The base field may be a subquery rather than a simple field.
        """
        try:
            return self.parent.fields_by_display_name[self.base_field_display_name]
        except KeyError:
            AdditiveField.logger.debug(
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
            return dtypes[self.sqla_func]
        except KeyError:
            AdditiveField.logger.debug(
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
    def name(self) -> FieldDisplayName:
        """Mimic field property"""
        return self.base_field.name

    @static_property
    def primary_key(self) -> bool:
        """Mimic field property"""
        return False

    @static_property
    def schema(self):
        """SqlAlchemy representation of the AdditiveField"""
        try:
            return self.aggregate_func(self.base_field.schema)\
                   .cast(self.sqla_dtype).label(self.display_name)
        except Exception as e:
            AdditiveField.logger.debug(
                'schema: Error creating aggregate field {}; error: {}'
                .format(self.display_name, str(e))
            )

    @static_property
    def sqla_dtype(self):
        """SqlAlchemy data type to cast to inside queries"""
        lkp = {
            'avg': Float(14, 2),
            'count': Integer,
            'sum': Float(14, 2)
        }
        try:
            return lkp[self.sqla_func]
        except KeyError:
            AdditiveField.logger.debug(
                'sqa_dtype: Unable to find sqa_dtype for AdditiveField {} '
                'sqa_func {}'.format(self.display_name, self.sqla_func)
            )

    @static_property
    def sqla_func(self) -> str:
        """The name of the SqlAlchemy name associated with the field"""
        try:
            return self.aggregate_func._FunctionGenerator__names[0]
        except KeyError:
            AdditiveField.logger.debug(
                'sqa_func: Error looking up sqa_func for AdditiveField {}'
                .format(self.display_name)
            )


@autorepr(str_attrs=['display_name', 'operator', '_value'],
          repr_attrs=['config', 'field'])
class Filter:
    """Holding tank for data needed to construct WHERE clause for field
    """
    logger = module_logger.getChild('Filter')
    table_name_index = defaultdict(dict)
    display_name_index = {}

    def __new__(cls, config: FilterConfig, field: 'Field'):
        """Filters for the same field on different views refer to the same
        filter object.
        """
        display_name = cls.compose_display_name(field, config.operator)
        fltr = cls.display_name_index.get(display_name)
        if not fltr:
            fltr = super().__new__(cls)
            cls.display_name_index[display_name] = fltr
        return fltr

    def __init__(self, config: FilterConfig, field) -> None:
        self.config = config
        self.field = field
        self.operator = config.operator
        self.default_value = config.default_value
        self._value = None  # type: Optional[SqlDataType]
        self.display_name = Filter.compose_display_name(field, config.operator)

        Filter.table_name_index[field.table_name][self.display_name] = self

    @staticmethod
    def compose_display_name(field: Field, operator: Operator) -> str:
        """Given a field and an operator, create a display name for the filter."""
        suffix = operator.suffix
        return field.display_name + (" " + suffix if suffix else "")

    @static_property
    def display_name(self) -> str:
        """The name of the filter to show the user."""
        suffix = self.operator.suffix
        return self.field.display_name + (" " + suffix if suffix else "")

    @property
    def filter(self) -> Optional[BinaryExpression]:
        """Create the SqlAlchemy binary expression for the WHERE clause"""
        if not self.is_filtered:
            return None
        else:
            try:
                fld = self.field.schema
                operator_mapping = {
                    # Boolean operators
                    Operator.bool_is:
                        lambda: self.field.schema == literal(self.value),
                    Operator.bool_is_not:
                        lambda: self.field.schema != literal(self.value),

                    # Numeric operators
                    Operator.number_equals:
                        lambda: fld == self.value,
                    Operator.number_does_not_equal:
                        lambda: fld != self.value,
                    Operator.number_greater_than:
                        lambda: fld > self.value,
                    Operator.number_greater_than_or_equal_to:
                        lambda: fld >= self.value,
                    Operator.number_less_than:
                        lambda: fld < self.value,
                    Operator.number_less_than_or_equal_to:
                        lambda: fld <= self.value,

                    # String operators
                    Operator.str_equals:
                        lambda: fld == self.value,
                    Operator.str_like:
                        lambda: fld.contains(self.value),
                    Operator.str_not_like:
                        lambda: fld.notlike('%{}%'.format(self.value)),
                    Operator.str_starts_with:
                        lambda: fld.startswith(self.value),
                    Operator.str_ends_with:
                        lambda: fld.endswith(self.value),

                    # Date operators
                    Operator.date_after:
                        lambda: func.date(fld) > self.value,
                    Operator.date_on_or_after:
                        lambda: func.date(fld) >= self.value,
                    Operator.date_before:
                        lambda: func.date(fld) < self.value,
                    Operator.date_on_or_before:
                        lambda: func.date(fld) <= self.value,
                    Operator.date_equals:
                        lambda: func.date(fld) == self.value,
                    Operator.date_does_not_equal:
                        lambda: func.date(fld) != self.value
                }  # type: Dict[Operator, Callable]
                return operator_mapping[self.operator]()
            except Exception as e:
                Filter.logger(
                    'filter: Error creating binary expression for filter; '
                    'error {}'.format(str(e))
                )

    @property
    def is_filtered(self) -> bool:
        """Is the current filter in use?  ie, does it have a value?"""
        if self.value == '':
            return False
        elif self.value is None:
            return False
        else:
            return True

    def __lt__(self, other) -> bool:
        """This method is needed to sort the filters in the query designer by
        their name."""
        return self.display_name < other.display_name

    @property
    def value(self) -> SqlDataType:
        """The current, validated value of the filter"""
        return self._value

    @value.setter
    def value(self, value: str) -> None:
        """The slot that the associated filter control sends messages to."""
        self._value = convert_value(
            field_type=self.field.dtype,
            value=value
        )

    @classmethod
    def find_by_display_name(cls, display_name: str) -> 'Filter':
        """Given a filter display name, return a reference to the associated
        filter"""
        try:
            return cls.display_name_index[display_name]
        except ValueError:
            Filter.logger.debug(
                'find_by_display_name: Could not find a filter named {}'
                .format(display_name)
            )

    @classmethod
    def find_by_table_name(cls, table_name: TableName) -> Dict[str, 'Filter']:
        """Given a table name, return a dictionary of its associated filter
        references indexed by their display name"""
        try:
            return cls.table_name_index[table_name]
        except ValueError:
            Filter.logger.debug(
                'find_by_table_name: Could not find filters for table {}'
                .format(table_name)
            )


@autorepr(str_attrs=['display_name'], repr_attrs=['config', 'parent'])
class CalculatedField:
    """A field that represents the combination of one or more fields in a Star.

    It mimics its base field except for the schema and editability
    """
    logger = module_logger.getChild('CalculatedField')

    def __init__(self, config: CalculatedFieldConfig, parent) -> None:
        self.config = config
        self.table_name = config.table_name
        self.formula = config.formula
        self.display_name = config.display_name
        self.show_on_fact_table = config.show_on_fact_table
        self.default_value = config.default_value
        self.visible = config.visible
        self.parent = parent

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
        return [Filter(flt_cfg, field=self) for flt_cfg in self.config.filters]

    @static_property
    def editable(self):
        """Mimic field property"""
        return False

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
            pypOptional(Literal('-')) + field_or_num
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
            return operator_lkp[op](evaluate_field(fld1), evaluate_field(fld2))
        return resolve_branches(self.parsed_formula)

    @static_property
    def field_format(self) -> FieldFormat:
        """Mimic field property"""
        if self.dtype == FieldType.Int:
            return FieldFormat.Int
        return FieldFormat.Accounting

    @static_property
    def filter_operators(self) -> List[BinaryExpression]:
        """Mimic field property"""
        return self.base_field.filter_operators

    @static_property
    def name(self) -> FieldDisplayName:
        """Mimic field property"""
        return self.display_name

    @static_property
    def schema(self) -> BinaryExpression:
        try:
            return self.evaluate_expression.label(self.display_name)
        except Exception as e:
            CalculatedField.logger.debug(
                'schema: Error creating schema for calculated field {}; '
                'error: {}'.format(self.display_name, str(e))
            )

    @static_property
    def primary_key(self) -> bool:
        """Mimic field property"""
        return False

    @static_property
    def star(self) -> 'Star':
        if not self._star:
            err_msg = "The star for CalculatedField {} was not" \
                      "injected prior to calling the field." \
                      .format(self.display_name)
            CalculatedField.logger.debug('star: {}'.format(err_msg))
            raise(AttributeError, err_msg)
        return self._star


@autorepr
class SummaryField(Field):
    """Concatenate multiple fields

    This field type is used for display on associated fact tables in lieu of
    their integer primary key.
    """
    logger = module_logger.getChild('SummaryField')

    def __init__(self, config: SummaryFieldConfig, dimension) -> None:
        super().__init__(config, table=dimension)
        self.config = config
        self.table_name = config.table_name
        self.display_name = config.display_name
        self.name = config.name
        self.separator = config.separator
        self.visible = config.visible
        self.dimension = dimension

    @static_property
    def filters(self) -> List[Filter]:
        return [Filter(flt_cfg, self) for flt_cfg in self.config.filters]

    @property
    def is_filtered(self) -> bool:
        for flt in self.filters:
            if flt.is_filtered:
                return True
        return False

    @static_property
    def display_fields(self) -> List[Column]:
        def lookup_field(fld_name: FieldName):
            """If the dimension is associated with another dimension
            use the schema for the summary field of that dimension as
            a part of the currend dimensions summary field schema."""
            fld = self.dimension.field(fld_name)
            if isinstance(fld, ForeignKey):
                try:
                    fk = Dimension.find_by_table_name(fld.dimension)
                    if fk:
                        return fk.summary_field
                    else:
                        raise KeyError(
                            'display_fields.fld_schema:  Could not find the '
                            'foreign key for field {}; dimension {}'
                            .format(fld_name, fld.dimension)
                        )
                except Exception as e:
                    SummaryField.logger.error(
                        'display_fields.fld_schema: Could not find a '
                        'dimension for fk field {}; table_name: {}; '
                        'error {}'
                        .format(fld.display_name, fld.dimension, str(e))
                    )
            return fld
        try:
            return [
                lookup_field(n)
                for n in self.config.display_fields
            ]
        except Exception as e:
            SummaryField.logger.error(
                'display_fields: Could not determine schema; error {}'
                .format(str(e))
            )
            raise

    @static_property
    def schema(self) -> Column:
        """Create the sqla schema to display for foreign key values
        on the one-side of the tables relationship with another."""
        return reduce(
            lambda x, y: x + self.config.separator + y,
            [fld.schema for fld in self.display_fields]
        ).label(self.config.display_name)

    def summarize_row(self, row: Tuple) -> Tuple:
        """When you add a row to a dimension, create the summary value in memory"""
        return reduce(
            lambda x, y: x + self.config.separator + y,
            [row[self.dimension.field_indices[fld]] for fld in self.display_fields]
        )


@autorepr
class Table:
    """A container to store fields

    This class is meant to be subclassed by Dimension and Fact table classes.
    """
    logger = module_logger.getChild('Table')
    table_name_index = {}  # type: Dict[TableName, 'Table']

    def __init__(self, config: Union[DimensionConfig, FactConfig]) -> None:
        self.config = config
        self.table_name = config.table_name
        self.display_name = config.display_name
        self.editable = config.editable
        self.display_rows = config.display_rows
        self.order_by = config.order_by
        self.show_on_load = config.show_on_load
        self.refresh_on_update = config.refresh_on_update

        Table.table_name_index[self.table_name] = self

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

    def delete_row(self, rid: int) -> Delete:
        """Create sql statement to delete a row, given a primary key"""
        return self.schema.delete().where(self.primary_key == rid)

    @lru_cache(maxsize=10)
    def field(self, name: FieldName) -> Field:
        """Look up a field based on it's name on the table."""
        try:
            return next(fld for fld in self.fields if fld.name == name)
        except StopIteration:
            Table.logger.debug(
                'field: Could not find table field named {} on table {}'
                .format(name, self.table_name)
            )

    @static_property
    def fields(self):
        return [Field(fld_cfg, self) for fld_cfg in self.config.fields] \
               + [ForeignKey(fk_cfg, self) for fk_cfg in self.config.foreign_keys]

    @static_property
    def field_indices(self) -> Dict[ColumnIndex, Field]:
        return {fld: ix for ix, fld in enumerate(self.fields)}

    @static_property
    def filters(self) -> List[Filter]:
        return [
            flt for fld in self.fields if fld.filters
            for flt in fld.filters
        ]

    @classmethod
    def find_by_table_name(cls, table_name: TableName) -> 'Table':
        try:
            return Table.table_name_index[table_name]
        except KeyError:
            Table.logger.debug(
                'find_by_table_name: Could not find a table named {}'
                .format(table_name)
            )

    @static_property
    def foreign_keys(self) -> List[Field]:
        return [
            ForeignKey(fk_cfg, self)
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
            Table.logger.debug(
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
            Table.logger.debug(
                'primary_key_index: could not find the primary key index for '
                'table {}'.format(self.table_name)
            )

    def save_changes(self, changes: Dict[str, List[tuple]]) -> Dict[str, int]:
        """Persist a change to the database.

        :param   changes: A dict keyed by the change type ('added', 'updated',
                 'deleted') and valued by the list of tuples representing rows
                 with changes of that type.
        :return: Returns a dict indexed by the change type ('added', 'updated',
                 'deleted') and valued by the count of said changes that were
                 successfully saved to the database.
        """
        trans = Transaction(con_str=self.config.db_path)
        new_rows_id_map = []  # type: List[Tuple[int, int]]
        try:
            for row in changes['deleted']:
                trans.execute(self.delete_row(row[self.primary_key_index]))

            for row in changes['added']:
                rid = row[self.primary_key_index]
                new_id = trans.execute(self.add_row(values=list(row)))
                new_rows_id_map.append((rid, new_id))

            for row in changes['updated']:
                rid = row[self.primary_key_index]
                trans.execute(self.update_row(pk=rid, values=list(row)))

            results = trans.commit()

            results['new_rows_id_map'] = new_rows_id_map
            return results
        except Exception as e:
            Table.logger.error('save_changes: Unable to save changes; error {}'
                .format(e))
            raise

    @static_property
    def schema(self) -> sqlaTable:
        """Map table to a sqlalchemy table schema"""
        try:
            cols = [fld.schema for fld in self.fields]
            return sqlaTable(self.table_name, md, *cols)
        except Exception as e:
            Table.logger.debug(
                'schema: Error creating the schema for table {}; error: {}'
                .format(self.table_name, e)
            )

    def update_row(self, *, pk: PrimaryKeyIndex, values: List[SqlDataType]) -> Update:
        """Statement to update a row on the table given the primary key value"""
        vals = {
            fld.name: convert_value(value=values[i], field_type=self.fields[i].dtype)
            for i, fld in enumerate(self.fields)
            if not fld.primary_key
        }
        return self.schema.update().where(self.primary_key == pk).values(vals)


@autorepr(str_attrs=['display_name'], repr_attrs=['config'])
class Dimension(Table):
    """Dimension table specifications"""
    logger = module_logger.getChild('Dimension')
    foreign_keys_index = defaultdict(dict)  # type: Dict[TableName, Dict[PrimaryKeyValue, str]]
    table_name_index = {}  # type: Dict[DimensionTableName, Table]

    def __init__(self, config: Union[DimensionConfig, LookupTableConfig]) -> None:
        self.config = config
        super(Dimension, self).__init__(config)

        self.refresh_on_update = config.refresh_on_update
        self.summary_field = SummaryField(config.summary_field, dimension=self)

        Dimension.table_name_index[self.table_name] = self
        self.pull_foreign_keys()

    @static_property
    def foreign_key_schema(self) -> Table:
        return select([
            self.primary_key.label('{}_{}'.format(self.table_name,
                                                  self.primary_key.name)),
            self.summary_field.schema
        ])

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

    def pull_foreign_keys(self) -> None:
        """Send select statement to QueryRunner to pull foreign keys associated
        with current dimension."""
        rows = fetch(qry=self.foreign_key_schema, con_str=self.config.db_path)
        Dimension.foreign_keys_index[self.table_name] = ValueSortedDict({
            row[0]: str(row[1]) for row in rows})

    @property
    def select(self, max_rows: int=1000) -> Select:
        """Only the dimension has a select method on the table class since
        the Fact table has to consider foreign keys so its select statement
        is composed at the Star level."""
        s = self.schema.select()
        filters_to_apply = (
            flt for flt
            in self.filters
            if flt.is_filtered
        )
        for f in filters_to_apply:
            s = s.where(f.filter)
        if self.order_by_schema:
            for o in self.order_by_schema:
                s = s.order_by(o)
        return s.limit(max_rows)

    @classmethod
    def find_foreign_keys_by_table_name(cls, dimension_table_name: TableName) -> Dict[PrimaryKeyValue, str]:
        """Look up the foreign keys associated with a dimension by its database
        table name."""
        try:
            return cls.foreign_keys_index[dimension_table_name]
        except KeyError:
            Dimension.logger.debug(
                'find_foreign_keys_by_table_name: Unable to find a '
                'foreign keys for a dimension named {}'
                .format(dimension_table_name))

    @classmethod
    def find_dimension_by_table_name(cls, dimension_table_name: DimensionTableName) -> Table:
        try:
            return cls.table_name_index[dimension_table_name]
        except KeyError:
            Dimension.logger.debug(
                'find_dimension_by_table_name: Unable to find a dimension '
                'named {}'.format(dimension_table_name)
            )

    def sync_foreign_keys(self, changes: Dict[str, List],
                                results: Dict[str, Union[int, List]]) -> None:
        for row in changes['deleted']:
            Dimension.foreign_keys_index[self.table_name].pop(
                row[self.primary_key_index], None)

        for row in changes['added']:
            try:
                id_map = results['new_rows_id_map']
                dummy_id = row[self.primary_key_index]
                # find the new id assigned by the database
                row_id = next(new_id for old_id, new_id in id_map
                              if old_id == dummy_id)
                # add the foreign key lookup kv pair to the foreign keys index
                Dimension.foreign_keys_index[self.table_name][row_id] = \
                    self.summary_field.summarize_row(row)
            except Exception as e:
                Dimension.logger.debug(
                    'sync_foreign_keys: error creating in-memory summary field'
                    'for dimension {}; row: {}; error: {}'
                    .format(self.table_name, row, e))
        for row in changes['updated']:
            try:
                row_id = row[self.primary_key_index]
                # delete old row
                Dimension.foreign_keys_index[self.table_name].pop(row_id, None)
                # add back row with new summary value
                Dimension.foreign_keys_index[self.table_name][row_id] = \
                    self.summary_field.summarize_row(row)
            except Exception as e:
                Dimension.logger.debug(
                    'sync_foreign_keys: error updating in-memory summary field'
                    'for dimension {}; row: {}; error: {}'
                    .format(self.table_name, row, e))

    def save_changes(self, changes: Dict[str, List[tuple]]):
        try:
            results = super().save_changes(changes)
        except:
            raise
        else:
            self.sync_foreign_keys(changes, results)
            return results


@autorepr
class ForeignKey(Field):
    logger = module_logger.getChild('ForeignKey')

    def __init__(self, config: ForeignKeyConfig, table) -> None:
        super().__init__(config, table=table)
        self.config = config
        self.dimension = config.dimension
        self.foreign_key_field = config.foreign_key_field  # name of id field on dim
        self.visible = config.visible
        self.table = table
        self.foreign_keys_fn = partial(Dimension.find_foreign_keys_by_table_name, self.dimension)

    @static_property
    def schema(self) -> Column:
        return Column(self.name, Integer, sqlaForeignKey("{t}.{f}".format(
            t=self.dimension, f=self.foreign_key_field)))

    def lookup_foreign_key(self, value: SqlDataType) -> str:
        try:
            return Dimension.find_foreign_keys_by_table_name(self.dimension).get(value)
        except Exception as e:
            ForeignKey.logger.debug(
                'lookup_foreign_key: Unable to find foreign key for dimension '
                '{}; foreign key: {}; error {}'
                .format(self.dimension, value, e)
            )


@autorepr
class LookupField(SummaryField):
    logger = module_logger.getChild('LookupField')
    table_name_index = defaultdict(list)

    def __init__(self, config: Dict, table):
        self.table_name = config.table_name
        self.display_name = config.display_name
        self.name = self.display_name
        self.dtype = FieldType.Str
        self.field_format = FieldFormat.Str
        self.editable = False
        self.dimension = self.table_name
        self.parent = table
        self.primary_key = False
        self.visible = True
        self.foreign_keys_fn = partial(Dimension.find_foreign_keys_by_table_name, self.table_name)

        self.proximal_dim_name = table.proximal_fk.dimension
        LookupField.table_name_index[self.proximal_dim_name].append(self)

    @property
    def distinct_values(self):
        return Dimension.find_by_table_name(self.table_name).lookup_keys

    @property
    def table(self):
        return Table.find_by_table_name(self.table_name)

    @property
    def foreign_keys(self):
        return Dimension.find_foreign_keys_by_table_name(self.table_name)

    @classmethod
    def find_lookup_fields_by_table_name(cls, table_name: TableName):
        try:
            return LookupField.table_name_index[table_name]
        except KeyError:
            LookupField.logger.debug(
                'find_lookup_fields_by_table_name: Could not find a lookup'
                'field referencing the table {}'.format(table_name)
            )

    def lookup_foreign_key(self, row_id: PrimaryKeyValue) -> str:
        try:
            return Dimension.find_foreign_keys_by_table_name(
                self.table_name).get(row_id)
        except Exception as e:
            LookupField.logger.debug(
                'lookup_foreign_key: Unable to find foreign key for lookup '
                'table {} for row id {}; error {}'
                .format(self.table_name, row_id, e)
            )

@autorepr
class LookupTable(Dimension):
    """Lookup table specifications

    We don't specify the maximum display or export rows since lookup tables
    should be (and in this case *must* have a low row count, and the user must
    be able to see the entire dimension to edit any foreign keys that may show
    up on the associated Fact table.
    """
    logger = module_logger.getChild('Dimension')

    def __init__(self, config: LookupTableConfig) -> None:
        self.config = config
        self.table_name = config.table_name
        self.id_field = Field(config.id_field, self)
        self.proximal_fk = ForeignKey(config.proximal_fk, table=self)
        self.distal_fk = ForeignKey(config.distal_fk, table=self)
        self.fields = [
            self.id_field,
            self.proximal_fk,
            self.distal_fk
        ]
        self.data = defaultdict(set)
        # self.lookup_keys = {}  # type: Dict[PrimaryKeyValue, PrimaryKeyValue]
        self.refresh_on_update = config.refresh_on_update
        self.lookup_field = LookupField(config.summary_field, table=self)
        Dimension.table_name_index[self.table_name] = self
        super().__init__(config=config)
        self.pull_foreign_keys()

    @property
    def distal_foreign_keys(self) -> Dict[PrimaryKeyValue, str]:
        distal_dim_name = self.distal_fk.dimension
        return Dimension.find_foreign_keys_by_table_name(distal_dim_name)

    @static_property
    def foreign_key_schema(self) -> Table:
        """Compose a select statment to create a lookup table for the current
        dimensions foreign key values."""
        return select([self.id_field.schema, self.proximal_fk.schema,
            self.distal_fk.schema]).select_from(self.schema)

    @property
    def foreign_keys(self) -> Optional[Dict[PrimaryKeyValue, str]]:
        if self.distal_foreign_keys and self.lookup_keys:
            return {
                key: '; '.join(sorted(self.distal_foreign_keys.get(val)
                                      for val in vals if val))
                for key, vals in self.lookup_keys.items()
            }
        return {}

    @property
    def lookup_keys(self):
        return {
            row[0]: [r[1] for r in row[1]]
            for row in groupby(sorted(self.data.values()), lambda x: x[0])
        }

    def pull_foreign_keys(self) -> None:
        self.data = {
            row[0]: (row[1], row[2])
            for row in fetch(qry=self.foreign_key_schema, con_str=self.config.db_path)
        }
        Dimension.foreign_keys_index[self.table_name] = self.foreign_keys

    def sync_foreign_keys(self, changes: Dict[str, List],
            results: Dict[str, Union[int, List]]) -> None:
        """Synchronize foreign key lookups for table

        The lookup table synchronization method does not account for updates
        since these should not be possible via the UI directly
        """
        for row in changes['deleted']:
            try:
                primary_key = row[0]
                LookupTable.logger.debug(
                    'deleting row {} from lookup table {}'
                    .format(row, self.table_name)
                )
                self.data.pop(primary_key)
            except Exception as e:
                LookupTable.logger.debug(
                    'sync_foreign_keys: error deleting foreign key '
                    'for lookup table {}; row: {}; error: {}'
                    .format(self.table_name, row, e))

        for row in changes['added']:
            try:
                id_map = results['new_rows_id_map']
                dummy_id = row[self.primary_key_index]
                # find the new id assigned by the database
                primary_key = next(
                    new_id for old_id, new_id in id_map
                    if old_id == dummy_id
                )
                LookupTable.logger.debug(
                    'adding row ({}, {}) to lookup table {}'
                    .format(row[1], row[2], self.table_name)
                )
                self.data[primary_key] = (row[1], row[2])
            except Exception as e:
                LookupTable.logger.debug(
                    'sync_foreign_keys: error adding foreign key '
                    'for lookup table {}; row: {}; error: {}'
                    .format(self.table_name, row, e))

    def save_changes(self, changes: Dict[str, List[tuple]]):
        try:
            results = Table.save_changes(self, changes)
        except:
            raise
        else:
            self.sync_foreign_keys(changes, results)
            Dimension.foreign_keys_index[self.table_name] = self.foreign_keys
            return results


@autorepr
class Fact(Table):
    """Fact table specification

    Fact tables are generally long, but not wide.  They primarily contain data
    that is aggregated, and references to dimension tables for contextual data.
    They may also contain 'junk' dimensions, which are simply dimensions that
    don't warrant a separate table to store them.
    """
    logger = module_logger.getChild('Fact')
    fact_table_name_index = {}

    def __init__(self, config: FactConfig) -> None:
        self.config = config
        super(Fact, self).__init__(config)
        self.refresh_on_update = config.refresh_on_update

        Fact.fact_table_name_index[self.table_name] = self

    @static_property
    def calculated_fields(self) -> List[CalculatedField]:
        """A field that is the product of a math operation on one or more
        fields
        """
        return [
            CalculatedField(calc_cfg, parent=self)
            for calc_cfg in self.config.calculated_fields
        ]

    @property
    def dimensions(self) -> List[DimensionTableName]:
        """List of the associated dimension names"""
        return [
            fld.dimension
            for fld in self.foreign_keys
        ]

    @classmethod
    def find_by_table_name(cls, fact_table_name: TableName) -> Table:
        """Look up a Fact table instance by it's database table name"""
        try:
            return Fact.fact_table_name_index[fact_table_name]
        except KeyError:
            Fact.logger.debug(
                'Fact.find_by_table_name: Could not find a fact table named {}'
                .format(fact_table_name)
            )

    @static_property
    def foreign_keys(self):
        return [fld for fld in self.fields if isinstance(fld, ForeignKey)]


@autorepr
class Star:
    """A Star is a Fact table with associated Dimensions

    A Star is a view for a fact table.  It inherits its editability
    from its core star.
    """
    logger = module_logger.getChild('Star')
    fact_table_index = {}

    def __init__(self, *, fact: Fact, dimensions: List[Dimension]=None) -> None:
        self.fact = fact  # type: Fact
        self._dimensions = dimensions  # type: List[Dimension]

        Star.fact_table_index[self.fact.table_name] = self

    @static_property
    def calculated_fields(self):
        if self.fact.calculated_fields:
            for fld in self.fact.calculated_fields:
                fld._star = self
            return [fld for fld in self.fact.calculated_fields]
        return []

    @static_property
    def dimensions(self) -> List[Dimension]:
        """Only include references to dimensions related to the Star's
        fact table by a foreign key."""
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
        return dict(ChainMap({}, fact_fields, dim_fields))

    @property
    def filters(self) -> List[Filter]:
        star_filters = []  # type: List[Filter]
        for dim in self.dimensions:
            for flt in dim.summary_field.filters:
                star_filters.append(flt)
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
    def refresh_on_update(self) -> bool:
        return self.fact.refresh_on_update

    @property
    def select(self) -> Select:
        """Override the Fact tables select method implementation to
        account for foreign key filters."""
        return self.star_query.limit(self.fact.display_rows)

    @static_property
    def summary_fields(self) -> Dict[FieldName, Field]:
        return {
            str(dim.summary_field.display_name): dim.summary_field
            for dim in self.dimensions
        }  # type: Dict[FieldName, Field]

    @static_property
    def table_name(self) -> TableName:
        return self.fact.table_name

    @property
    def joined_dimensions(self) -> List[Dimension]:
        #dimensions with a filter on them
        filtered_dims = [
            dim for dim in self.dimensions
            if dim.summary_field.is_filtered
        ]
        #dimensions included in the order by clause
        order_by_dims = []
        for order_by in self.order_by:
            if order_by.field_name in self.summary_fields.keys():
                summary_field = self.summary_fields[order_by.field_name]
                dim = summary_field.dimension
                if dim not in filtered_dims:
                    order_by_dims.append(dim)
        # join on dimensions involved in a calculated field
        # TODO
        return list(chain(filtered_dims, order_by_dims))

    @static_property
    def order_by(self) -> Optional[List[UnaryExpression]]:
        return self.fact.order_by

    @static_property
    def order_by_schema(self) -> Optional[List[UnaryExpression]]:
        """Return the order by fields for the Star"""
        if not self.order_by:
            return

        def lkp_sort_order(order_by: OrderBy):
            if order_by.field_name in self.summary_fields.keys():
                fld = self.summary_fields[order_by.field_name].schema
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
    def star_query(self) -> Select:
        try:
            calculated_fields = [
                fld.schema
                for fld in self.calculated_fields
            ]
            fact = self.fact.schema  # type: sqa.Table
            star = fact

            # join on dimensions with a filter on them
            # filtered_dims = [
            #     dim for dim in self.dimensions
            #     if dim.summary_field.is_filtered
            # ]
            # if filtered_dims:
            #     for dim in filtered_dims:
            #         star = star.join(dim.schema)
            #         star = star.join(dim.foreign_key_schema)
            # # join on dimensions included in the order by clause
            # for order_by in self.order_by:
            #     if order_by.field_name in self.summary_fields.keys():
            #         summary_field = self.summary_fields[order_by.field_name]
            #         dim = summary_field.dimension
            #         if dim not in filtered_dims:
            #             star = star.join(summary_field.dimension.schema)
            # # join on dimensions involved in a calculated field
            # # TODO
            for dim in self.joined_dimensions:
                star = star.join(dim.schema)
                star = star.join(dim.foreign_key_schema)

            if calculated_fields:
                fields = fact.columns + calculated_fields
            else:
                fields = fact.columns
            qry = select(fields).select_from(star)
            for f in [flt for flt in self.filters if flt.is_filtered]:
                qry = qry.where(f.filter)
            if self.order_by_schema:
                for o in self.order_by_schema:
                    qry = qry.order_by(o)
            return qry
        except Exception as e:
            Star.logger.error(
                'star_query: Error composing star query: {}'
                .format(str(e))
            )
            raise

    @classmethod
    def find_by_fact_table_name(cls, fact_table_name: FactTableName) -> 'Star':
        try:
            return Star.fact_table_index[fact_table_name]
        except KeyError:
            Star.logger.debug(
                'find_by_fact_table_name: Could not find a Star with a '
                'fact table named {}'.format(fact_table_name)
            )


@autorepr
class View:
    """An aggregate view over a Star
    """
    logger = module_logger.getChild('View')
    display_name_index = {}

    def __init__(self, config: ViewConfig, star: Star) -> None:
        self.config = config
        self.display_name = config.view_display_name  # type: str
        self.fact_table_name = config.fact_table_name  # type: str
        self.table_name = self.fact_table_name  # TODO
        self.primary_key_index = -1
        self.editable = False
        self.show_on_load = config.show_on_load
        self.order_by = config.order_by
        self.star = star

        View.display_name_index[self.display_name] = self

    @static_property
    def additive_fields(self) -> List[AdditiveField]:
        if self.config.additive_fields:
            return [
                AdditiveField(config=add_cfg, parent=self.star)
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
            View.logger.debug(
                'field_by_display_name: Error looking up field_by_display_'
                'name; could not find a field named {} in the View {}'
                .format(display_name, self.display_name)
            )
        except Exception as e:
            View.logger.debug(
                'field_by_display_name: Error looking up field_by_display_'
                'name: err {}'.format(str(e))
            )

    @static_property
    def filters(self) -> List[Filter]:
        """List of filters on the Star this View is based on."""
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
    def order_by_schema(self) -> List:
        """Return the order by fields for the View"""
        def lkp_sort_order(order_by: OrderBy):
            try:
                fld = self.field_by_display_name(order_by.field_name)
                if order_by.sort_order == SortOrder.Ascending:
                    return fld.schema.asc()
                return fld.schema.desc()
            except KeyError:
                View.logger.debug(
                    'order_by_schema: Unable to look up sort order for View {}, '
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
            # TODO: joined_dimensions should account for dims associated with group-by fields
            for dim in self.star.joined_dimensions:
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
            View.logger.debug(
                'select: Error composing select statement for View {}; '
                'error {}'.format(self.display_name, str(e)))

    @classmethod
    def find_by_display_name(cls, view_display_name: TableDisplayName) -> 'View':
        try:
            return View.display_name_index[view_display_name]
        except KeyError:
            View.logger.debug(
                'find_by_display_name: Could not find a view named {}'
                .format(view_display_name)
            )


@autorepr(str_attrs=['app', 'table'], repr_attrs=['app', 'table'])
class DisplayPackage:
    """This class stores the specifications to send to the view

    :param table: Base table, star, or view that the queries are created from.
    """
    table_name_index = weakref.WeakValueDictionary()

    def __init__(self, *, app, table: Table) -> None:
        self.app = app
        self.table = table

        DisplayPackage.table_name_index[self.table.table_name] = self

    @static_property
    def display_base(self) -> Union[Star, View, Table]:
        bases = {
            Fact: lambda: Star.find_by_fact_table_name(self.table.table_name),
            View: lambda: View.find_by_display_name(self.table.display_name),
            Dimension: lambda: self.table,
            LookupTable: lambda: self.table
        }
        return bases[type(self.table)]()

    @static_property
    def display_name(self) -> TableDisplayName:
        return self.display_base.display_name

    @static_property
    def editable_field_indices(self) -> List[ColumnIndex]:
        return [ix for ix, fld in self.fields_by_original_index.items()
                if fld.editable]

    @static_property
    def fields_by_display_index(self) -> Dict[ColumnIndex, Field]:
        field_partitions = {
            FieldFormat.Accounting: [],
            FieldFormat.Bool:       [],
            FieldFormat.Currency:   [],
            FieldFormat.Date:       [],
            FieldFormat.DateTime:   [],
            FieldFormat.Float:      [],
            FieldFormat.Int:        [],
            FieldFormat.Str:        [],
            FieldFormat.Memo:       []
        }
        for i, fld in enumerate(self.fields):
            field_partitions[fld.field_format].append(fld)

        return SortedDict({
            0 if fld.primary_key else ix: fld
            for ix, fld in enumerate(chain(
                sorted(field_partitions[FieldFormat.Str],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.Date],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.DateTime],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.Bool],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.Int],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.Currency],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.Float],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.Accounting],
                    key=lambda x: x.display_name),
                sorted(field_partitions[FieldFormat.Memo],
                    key=lambda x: x.display_name)
            ), start=1)
        })

    @static_property
    def display_index_by_field(self):
        return {v: k for k, v in self.fields_by_display_index.items()}

    @static_property
    def fields_by_original_index(self) -> Dict[ColumnIndex, Field]:
        return SortedDict({ix: fld for ix, fld in enumerate(self.fields)})

    @static_property
    def field_order_map(self):
        return SortedDict({
            original_ix: self.display_index_by_field[fld]
            for original_ix, fld in self.fields_by_original_index.items()
        })

    @static_property
    def foreign_key_fields(self) -> List[Field]:
        return [
            fld for fld in self.fields
            if isinstance(fld, ForeignKey) or isinstance(fld, LookupField)
        ]

    @property
    def foreign_keys_by_original_index(self):
        return SortedDict({
            self.original_index_by_field[fld]: fld.foreign_keys_fn
            for fld in self.foreign_key_fields
        })

    @static_property
    def foreign_key_indices(self):
        return self.foreign_keys_by_original_index.keys()

    def foreign_key_lookup(self, val: PrimaryKeyValue, col: ColumnIndex) \
            -> SqlDataType:
        if col in self.lookup_field_indices:
            return val
        elif col in self.foreign_key_indices:
            try:
                return self.foreign_keys_by_original_index[col]()[val]
            except KeyError:
                return ''
        else:
            return val

    @static_property
    def lookup_field_display_packages_by_original_index(self) \
            -> Dict[ColumnIndex, 'DisplayPackage']:
        return {
            self.original_index_by_field[fld]:
                DisplayPackage.find_display_package_by_table_name(
                    fld.parent.table_name)
            for fld in self.lookup_fields
        }

    @static_property
    def original_index_by_field(self) -> Dict[Field, ColumnIndex]:
        return {v: k for k, v in self.fields_by_original_index.items()}

    @static_property
    def lookup_fields(self) -> List[LookupField]:
        return LookupField.find_lookup_fields_by_table_name(
            self.table.table_name)

    @property
    def lookup_values_by_original_index(self) \
            -> Dict[ColumnIndex, Dict[PrimaryKeyValue, str]]:
        return SortedDict({
            self.original_index_by_field[fld]: partial(
                Dimension.find_foreign_keys_by_table_name, fld.table_name)
            for fld in self.lookup_fields
        })

    @static_property
    def lookup_field_indices(self):
        return self.lookup_values_by_original_index.keys()

    @static_property
    def fields(self) -> List[Field]:
        return list(chain(self.display_base.fields, self.lookup_fields))

    @static_property
    def fields_by_display_name(self):
        return SortedDict({fld.display_name: fld for fld in self.fields})

    @property
    def filters(self) -> List[Filter]:
        if isinstance(self.table, Fact):
            return self.display_base.filters
        return self.table.filters

    def find_foreign_keys_by_display_name(self, display_name):
        fk = self.fields_by_display_name[display_name]
        return fk.foreign_keys

    def find_distinct_values_by_display_name(self, display_name):
        fk = self.fields_by_display_name[display_name]
        return fk.distinct_values

    @static_property
    def primary_key_index(self) -> ColumnIndex:
        return self.table.primary_key_index

    @classmethod
    def find_display_package_by_table_name(cls, table_name: TableName) \
            -> 'DisplayPackage':
        try:
            return DisplayPackage.table_name_index[table_name]
        except KeyError:
            DisplayPackage.logger.debug(
                'find_display_package_by_table: Could not find a display '
                'package associated with table {}'.format(table_name)
            )

    @static_property
    def visible(self) -> bool:
        if isinstance(self.display_base, LookupTable):
            return False
        return True


@autorepr
class App:
    """Configuration settings for application-wide settings"""
    def __init__(self, config: AppConfig) -> None:
        self.color_scheme = config.color_scheme
        self.db_path = config.db_path
        self.display_name = config.display_name


class Constellation:
    """Collection of all the Stars in the application"""
    logger = module_logger.getChild('Constellation')

    def __init__(self, config: ConstellationConfig) -> None:
        self.config = config

    @static_property
    def app(self) -> App:
        return App(self.config.app)

    @static_property
    def dimensions(self) -> List[Dimension]:
        return [
            Dimension(config=dim_cfg)
            for dim_cfg in self.config.dimensions
        ]

    @static_property
    def facts(self) -> List[Fact]:
        return [
            Fact(fact_cfg)
            for fact_cfg in self.config.facts
        ]

    @static_property
    def lookup_tables(self) -> List[LookupTable]:
        return [
            LookupTable(config=lkp_cfg)
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
    def display_packages(self) -> List[DisplayPackage]:
        """Create a package to send to the view for display"""
        packages = []
        for tbl in self.tables:
            try:
                display_pkg = DisplayPackage(app=self.app, table=tbl)
                packages.append(display_pkg)
            except Exception as e:
                Constellation.logger.error(
                    'display_packages: Unable to create display package for '
                    'table {}; error {}'.format(tbl.table_name, str(e))
                )
        return packages

    @static_property
    def views(self) -> List[View]:
        try:
            return [
                View(config=view_cfg, star=self.star(view_cfg.fact_table_name))
                for view_cfg in self.config.views
            ]
        except Exception as e:
            Constellation.logger.error(
                "views: Error creating views; error: {}"
                .format(str(e))
            )

    @lru_cache(maxsize=10)
    def star(self, fact_table: FactTableName) -> Star:
        """Return the specific Star system localized on a specific Fact table"""
        try:
            return self.stars[fact_table]
        except KeyError:
            Constellation.logger.debug(
                'star: The fact table {} could not be found'
                .format(fact_table)
            )

    @static_property
    def stars(self) -> Dict[FactTableName, Star]:
        return {
            fact.table_name: Star(fact=fact, dimensions=self.dimensions)
            for fact in self.facts
        }


@lru_cache(maxsize=5)
def get_constellation(json_path: Optional[str]=None) -> Constellation:
    if json_path:
        config = get_config(json_path)
        constellation_config = ConstellationConfig(config)
        return Constellation(constellation_config)
    else:
        return Constellation(default_config)

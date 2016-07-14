"""This module is responsible for procuring data for the model to prep and send to view.

"""
from collections import OrderedDict
import os
import re
import string
from typing import Any, Dict, List, Set, Tuple, Union

from json_config import SimpleJsonConfig
from PyQt4 import QtCore

from query_exporter import QueryExporter
from logger import log_error
from query_runner import QueryRunner
from typing import NamedTuple
from utilities import sqlite_exec

Field = NamedTuple('Field', [('name', str), ('type', str)])
Filter = NamedTuple('Filter', [('field', str), ('operator', str), ('type', str)])
NumberRangeType = Union[int, float]
Operator = NamedTuple('Operator', [('options', List[str]), ('default', str)])


def operator_options(field: Field) -> Operator:
    """Look up valid operator types based on the type of the field."""
    operators = {
        'date':    Operator(options=['=', '!=', '>=', '<='], default='=')
        , 'float': Operator(
            options=['+/- 0.01', '=', '!=', '>=', '>', '<=', '<'],
            default='+/- 0.01')
        , 'int':   Operator(options=['+/- 1', '=', '!=', '>=', '>', '<=', '<'],
            default='=')
        , 'str':   Operator(
            options=['=', '!=', 'Like', 'Starts with', 'Ends with'],
            default='Starts with')
    }
    try:
        return operators[field.type]  # type: OperatorType
    except IndexError:
        return Operator(options=[], default='')


@log_error
def where_condition(field: Field, value: str, operator: str) -> Tuple[str, str]:
    """Given a field, operator, and value, return a valid sql where condition.

    This function returns a tuple where the first item is the where
    condition, and the second is an error message if the inputs were
    found to be invalid.

    Examples:
        >>> fld = Field(name='a', type='int', filterable=True)
        >>> print(where_condition(fld, value='o', operator='='))
        (None, 'Unable to parse criteria: field=a, operator==, value=o: value is not numeric')
    """
    def date_range(rng: int, field_name: str, inclusive=True):
        return "{n} {lop} date('{v}', '+{r} day') " \
            "AND {n} {gop} date('{v}', '-{r} day')".format(
            n=field_name, lop='<=' if inclusive else '<'
            , gop='>=' if inclusive else '>', v=value, r=rng
        )

    def err_msg(msg: str='') -> str:
        return "Unable to parse criteria: field={n}, operator={o}, value={v}: {m}"\
            .format(n=field.name, o=operator, v=value, m=msg)

    def number_range(rng: NumberRangeType, field_name: str, inclusive=True) -> str:
        return "{n} {l} {v} + {r} AND {n} {g} {v} - {r}".format(
            n=field_name, l='<=' if inclusive else '<'
            , g='>=' if inclusive else '>', v=value, r=rng
        )

    rangeops = {
        '+/- 0.01': number_range(rng=0.01, field_name=field.name, inclusive=True)
        , '+/- 1': number_range(rng=1, field_name=field.name, inclusive=True)
        , 'Same day': date_range(rng=1, field_name=field.name, inclusive=False)
    }  # type: Dict[str, str]
    textops = {
        'Like': "{n} LIKE '%{v}%'".format(n=field.name, v=value)
        , 'Starts with': "{n} LIKE '{v}%'".format(n=field.name, v=value)
        , 'Ends with': "{n} LIKE '%{v}'".format(n=field.name, v=value)
    }
    if not value:
        return '', ''
    elif field.type == 'str':
        val = value.replace("'", "''")
        if val.isalnum() and all(c in string.printable for c in val):
            if operator in textops.keys():
                return textops.get(operator), ''
            return "{n} {o} '{v}'".format(n=field.name, o=operator, v=val), ''
        return '', err_msg("value is not a valid search string")
    elif field.type == 'date':
        if re.match(r"^\d{4}-\d{2}-\d{2}.*$", value):
            if operator in rangeops.keys():
                return rangeops.get(operator), ''
            return "{n} {o} '{v}'".format(n=field.name, o=operator, v=value), ''
        return '', err_msg()
    elif field.type in ('int', 'float'):
        if not re.match(r"^[0-9]*[.]?[0-9]*$", value):
            return '', err_msg('value is not numeric')
        try:
            val = float(value)
            if operator in rangeops.keys():
                return rangeops.get(operator), ''
            return "{n} {o} {v}".format(n=field.name, o=operator, v=val), ''
        except:
            return '', err_msg('value is not numeric')
    return '', err_msg()


@log_error
def query_manager_config(path: str) -> dict:
    """Read config variables from json into a dict for the QueryManager to consume.

    The query manager doesn't pull it's own configuration primarily because
    we want to make it easy to mock out during testing.
    """
    cfg = SimpleJsonConfig(json_path=path)
    config = {
        'db': cfg.get_or_set_variable(key='db_path', default_value='test.db')
        , 'fields': cfg.get_or_set_variable(
            'fields', [
                ['CustomerID', 'int']
                , ['ProductID', 'int']
                , ['CustomerName', 'str']
                , ['OrderDate', 'date']
                , ['ShippingDate', 'date']
                , ['ShippingAddress', 'str']
                , ['SalesAmount', 'float']
            ]
        )
        , 'primary_key': cfg.get_or_set_variable('primary_key', 'ID')
        , 'filters': cfg.get_or_set_variable(
            'filters', [
                ['OrderDate', '>=']
                , ['OrderDate', '<=']
                , ['CustomerName', 'Starts with']
                , ['SalesAmount', '+/- 0.01']
            ]
        )
        , 'table': cfg.get_or_set_variable(
            key='table'
            , default_value='SalesHistory'
        )
        , 'max_export_rows': cfg.get_or_set_variable(
            'max_export_rows'
            , 500000
        )
        , 'max_display_rows': cfg.get_or_set_variable(
            'max_display_rows'
            , 1000
        )
        , 'order_by': cfg.get_or_set_variable(
            'order_by'
            , ''
        )
    }

#   Validate configuration settings
    for f in config['filters']:
        if f[0] not in [fld[0] for fld in config['fields']]:
            raise ValueError("""One or more filter names weren't
                found in the field list.""")
    return config


class QueryManager(QtCore.QObject):
    """Create a query from user input."""

    error_signal = QtCore.pyqtSignal(str)
    exit_signal = QtCore.pyqtSignal()
    query_results_signal = QtCore.pyqtSignal(list)
    rows_returned_signal = QtCore.pyqtSignal(str)
    rows_exported_signal = QtCore.pyqtSignal(int)

    def __init__(self, config: dict) -> None:
        super(QueryManager, self).__init__()

        self.config = config  # type: Dict[str, Any]
        self._criteria = {}  # type: Dict[str, str]
        self._exporter = QueryExporter()
        self._runner = QueryRunner()

    #   Configuration settings that users are able to change at runtime
        self._filters = []  # type: List[Filter]
        self._max_display_rows = 0
        self._max_export_rows = 0
        self._order_by = ""

    #   Connect Signals
        self._exporter.signals.error.connect(
            self.error_signal.emit)
        self._exporter.signals.error.connect(
            self.error_signal.emit)
        self._exporter.signals.rows_exported.connect(
            self.rows_exported_signal.emit)
        self.exit_signal.connect(
            self._exporter.signals.exit.emit)
        self.exit_signal.connect(
            self._runner.signals.exit.emit)
        self._runner.signals.error.connect(
            self.error_signal.emit)
        self._runner.signals.results.connect(
            self.query_results_signal.emit)
        self._runner.signals.rows_returned_msg.connect(
            self.rows_returned_signal.emit)

    @log_error
    def add_criteria(self, field: Field, value: str, operator: str) -> None:
        """Accept a string with a type and convert it into a where condition"""
        operator = operator or operator_options(field).default
        if value:
            self._criteria[field.name + "_" + operator] = where_condition(
                field=field
                , value=value
                , operator=operator
            )[0]
        else:
            try:
                del self._criteria[field.name]
            except KeyError:
                pass

    @property
    def criteria(self) -> Dict[str, str]:
        return self._criteria or {}

    @property
    def db(self) -> str:
        return self.config.get('db')

    @property
    def order_by(self) -> str:
        if self._order_by:
            return self._order_by
        return '{} {}'.format(self.config.get('order_by'), 'asc')

    @order_by.setter
    def order_by(self, fieldname: str, asc_desc: str='asc') -> None:
        self._order_by = '{} {}'.format(fieldname, asc_desc)

    def export(self) -> None:
        self._exporter.start_pull(
            sql=self.sql_export
            , db_path=self.config.get('db')
        )

    @property
    def fields(self) -> Dict[int, Field]:
        """Return a dictionary of Field tuples (name, type, filterable)."""

        return OrderedDict({
            i: Field(name=val[0], type=val[1])
            for i, val
            in enumerate(self.config.get('fields'))
        })

    @property
    def field_types(self) -> Dict[str, str]:
        """Dictionary of field names with their associated field type."""
        return {
            val[0]: val[1]  # field name: field type
            for val in self.fields.values()
        }

    @property
    def filters(self) -> List[Filter]:
        if self._filters:
            return sorted(self._filters)
        return sorted(
            Filter(
                field=f[0]
                , operator=f[1]
                , type=self.field_types.get(f[0])
            )
            for f in self.config.get('filters')
        )

    @property
    def headers(self) -> List[str]:
        """Return a list of field headers for display."""
        if self.fields:
            return [fld.name for fld in self.fields.values()]
        return []

    @property
    def max_display_rows(self) -> int:
        if self._max_display_rows > 0:
            return self._max_display_rows
        if self.config.get('max_display_rows') > 0:
            return self.config.get('max_display_rows')
        return 0

    @property
    def max_display_rows_clause(self) -> str:
        if self.max_display_rows > 0:
            return "LIMIT {}".format(self.max_display_rows)
        return ""

    @property
    def max_export_rows(self) -> int:
        if self._max_export_rows > 0:
            return self._max_export_rows
        elif self.config.get('max_export_rows') > 0:
            return self.config.get('max_export_rows')
        else:
            return 0

    @property
    def max_export_rows_clause(self) -> str:
        if self.max_export_rows > 0:
            return "LIMIT {}".format(self.max_export_rows)
        return ""

    @property
    def order_by(self) -> str:
        if self._order_by:
            return 'ORDER BY {}'.format(self.config.get('order_by'))
        else:
            return ''

    @log_error
    def pull(self) -> None:
        self._runner.run_sql(
            query=self.sql_display
            , database_path=self.db
            , fields=self.fields
            , max_rows=self.max_display_rows
        )

    @property
    def primary_key(self):
        return self.config.get('primary_key')

    def reset(self) -> None:
        self._criteria = {}

    @property
    def table(self) -> str:
        if re.match('.*[.]sql$', self.config.get('table')):
        # table name == path to a sql query
            fp = os.path.join('sql', self.config.get('table'))
            with open(fp, 'r') as fh:
                qry = ' '.join([
                    line.replace(r'\n', '')
                    for line
                    in fh.readlines()
                ])
                return '({})'.format(qry)
        return self.config.get('table')

    @property
    def select_statement(self) -> str:
        fieldnames = [val.name for val in self.fields.values()]
        return "SELECT {fields} FROM {table}".format(
            fields=", ".join(fieldnames)
            , table=self.table
        )

    @property
    def sql_display(self) -> str:
        return ' '.join((
            self.select_statement
            , self.where_clause
            , self.order_by
            , self.max_display_rows_clause
        ))

    @property
    def sql_export(self) -> str:
        return ' '.join((
            self.select_statement
            , self.where_clause
            , self.order_by
            , self.max_export_rows_clause
        ))

    @property
    def str_criteria(self) -> str:
        criteria = set()  # type: Set[str]
        [
            criteria.add(c)
            for c
            in self.criteria.values()
            if c
        ]
        if criteria:
            return '(' + '; '.join(criteria) + ')'
        return 'top {} rows'.format(self.max_display_rows)

    @log_error
    def update(self, record_id: int, values: set) -> str:
        """Persist a change to the data store.
        """
        sqlite_exec("""
            UPDATE {table}
            SET
            WHERE {id_field} = {id_val}
        """)

    @property
    def where_clause(self) -> str:
        criteria = set()  # type: Set[str]
        [
            criteria.add(c)
            for c
            in self.criteria.values()
            if c
        ]
        if criteria:
            return 'WHERE ' + ' AND '.join(criteria)
        else:
            return ''


if __name__ == "__main__":
    import doctest
    doctest.testmod()

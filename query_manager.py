"""This module is responsible for procuring data for the model to prep and send to view.

"""
from collections import OrderedDict
import os
import re
import string
from typing import Any, Dict, List, Set, Tuple, Union

from json_config import SimpleJsonConfig
from PyQt4 import QtCore

from json_config import JsonType
from query_exporter import QueryExporter
from logger import log_error
from query_runner import QueryRunner
from typing import NamedTuple
from utilities import cache, inspect_table, SQLiteConnection, sqlite_pull, immutable_property

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
@cache
def query_manager_config(path: str) -> dict:
    """Read config variables from json into a dict for the QueryManager to consume.

    The query manager doesn't pull it's own configuration primarily because
    we want to make it easy to mock out during testing.
    """
    cfg = SimpleJsonConfig(json_path=path)
    config = {
        'allowed_operations': cfg.get_or_set_variable(
            'allowed_operations'
            , ['add', 'delete', 'update']
        )
        , 'db': cfg.get_or_set_variable(key='db_path', default_value='test.db')
        , 'fields': cfg.get_or_set_variable(
            'fields', [
                {'name': 'CustomerID', 'dtype': 'int'}
                , {'name': 'ProductID', 'dtype': 'int'}
                , {'name': 'CustomerName', 'dtype': 'str'}
                , {'name': 'OrderDate', 'dtype': 'date'}
                , {'name': 'ShippingDate', 'dtype': 'date'}
                , {'name': 'ShippingAddress', 'dtype': 'str'}
                , {'name': 'SalesAmount', 'dtype': 'float'}
            ]
        )
        , 'primary_key': cfg.get_or_set_variable(
                'primary_key'
                , 'CustomerID'
        )
        , 'editable_fields': cfg.get_or_set_variable(
            'editable_fields', [
                'OrderDate'
                , 'CustomerName'
                , 'ProductID'
            ]
        )
        , 'filters': cfg.get_or_set_variable(
            'filters', [
                {'name': 'OrderDate', 'default_operator': '>='},
                {'name': 'OrderDate', 'default_operator': '<='},
                {'name': 'CustomerName', 'default_operator': 'Starts with'},
                {'name': 'SalesAmount', 'default_operator': '+/- 0.01'}
            ]
        )
        , 'foreign_keys': cfg.get_or_set_variable(
            'foreign_keys',
            [
                {
                    'fk_on_main_table': 'ProductID',  # foreign key name on main table
                    'source_field_to_display': "ProductCategory || ' - ' || ProductName", # source table display field(s)
                    'foreign_table': 'Products',  # display name on datasheet
                    'foreign_key_id': 'ID'  # primary key on source table for the foreign key
                }
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
        if f['name'] not in {fld['name'] for fld in config['fields']}:
            raise ValueError("""One or more filter names weren't found in the field list.""")

    required_settings = {'db', 'fields', 'primary_key', 'table'}
    for s in required_settings:
        if not config[s]:
            raise ValueError("The config file is missing a required setting: {}".format(s))

    return config


class QueryManager(QtCore.QObject):
    """Create a query from user input.

    Foreign key tables should be dim tables (ie. small in the number of rows.
        This is important since the foreign key lookups are stored in memory for
        efficiency purposes.
    The main table should be the fact table (ie, large number of rows with
        fields to aggregate.
    """

    error_signal = QtCore.pyqtSignal(str)
    exit_signal = QtCore.pyqtSignal()
    query_results_signal = QtCore.pyqtSignal(list)
    row_updated_signal = QtCore.pyqtSignal(str, tuple, int)
    rows_returned_signal = QtCore.pyqtSignal(str)
    rows_exported_signal = QtCore.pyqtSignal(int)

    def __init__(self, config: Dict[str, JsonType]) -> None:
        super(QueryManager, self).__init__()

        self.config = config  # type: Dict[str, JsonType]
        self._exporter = QueryExporter()
        self._runner = QueryRunner()
        self._primary_table_fields = []  # saved here to minimize IO
        self._foreign_keys = {} # saved here to minimize IO
        # self._main_table_fields = []

    #   Configuration settings that users are able to change at runtime
        self._criteria = {}  # type: Dict[str, str]
        self._filters = []  # type: List[Filter]

    #   Connect Signals
        self._exporter.signals.error.connect(self.error_signal.emit)
        self._exporter.signals.error.connect(self.error_signal.emit)
        self._exporter.signals.rows_exported.connect(self.rows_exported_signal.emit)
        self.exit_signal.connect(self._exporter.signals.exit.emit)
        self.exit_signal.connect(self._runner.signals.exit.emit)
        self._runner.signals.error.connect(self.error_signal.emit)
        # self._runner.signals.results.connect(self.query_results_signal.emit)
        self._runner.signals.results.connect(self.process_results)
        self._runner.signals.rows_returned_msg.connect(self.rows_returned_signal.emit)

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

    def add_row(self, row=tuple) -> int:
        """Append row to database and return id value of insert"""
        main_table_fields = (fld.name for fld in self.main_table_fields)
        row_dict = {
            i: val
            for i, val
            in enumerate(row)
            if self.fields[i].name in main_table_fields
                and i != self.primary_key_index
        }
        for i in self.foreign_keys.keys():
            row_dict[i] = self.reverse_fk_lookup(ix=i, val=row_dict[i])

        field_dict = {
            self.fields[k].name: v
            for k, v in row_dict.items()
        }
        sql = ("INSERT INTO {t} (" + ", ".join(field_dict.keys())
            + ") VALUES (" + ", ".join(":" + fld for fld in field_dict.keys()) + ")"
        ).format(t=self.table)
        with SQLiteConnection(self.db, read_only=False) as con:
            cur = con.cursor()
            cur.execute(sql, field_dict)
            return cur.lastrowid

    @property
    def criteria(self) -> Dict[str, str]:
        return self._criteria or {}

    @property
    def db(self) -> str:
        return self.config['db']

    def delete_row(self, id: int) -> None:
        with SQLiteConnection(db_name=self.db, read_only=False) as con:
            con.execute(
                "DELETE FROM {t} WHERE {n} = {v}"
                    .format(t=self.table, n=self.primary_key, v=id)
            )

    @property
    def editable(self) -> bool:
        return 'add' in self.config['allowed_operations']

    @property
    def editable_fields(self) -> List[int]:
        return [
            self.get_field_index(fld)
            for fld in self.config['editable_fields']
            if fld != self.primary_key
        ]

    def get_field_index(self, name: str) -> int:
        return min(i for i, fld in enumerate(self.fields.values()) if fld.name == name)

    def export(self) -> None:
        self._exporter.start_pull(
            sql=self.sql_export
            , db_path=self.config.get('db')
        )

    @immutable_property
    def fields(self) -> Dict[int, Field]:
        """Return a dictionary of Field tuples (name, type, filterable)."""
        return OrderedDict({
            i: Field(name=val['name'], type=val['dtype'])
            for i, val
            in enumerate(self.config.get('fields'))
        })

    @property
    def field_types(self) -> Dict[str, str]:
        """Dictionary of field names with their associated field type."""
        return {
            val.name: val.type  # field name: field type
            for val in self.fields.values()
        }

    @property
    def filters(self) -> List[Filter]:
        if self._filters:
            return sorted(self._filters)
        return sorted(
            Filter(
                field=f['name']
                , operator=f['default_operator']
                , type=self.field_types.get(f['name'])
            )
            for f in self.config.get('filters')
        )

    @property
    def foreign_keys(self) -> Dict[int, Dict[int, str]]:
        if self._foreign_keys:
            return self._foreign_keys
        fks = self.config['foreign_keys']
        for fk_cfg in fks:
            sql = "SELECT {i}, {s} FROM {t}".format(
                i=fk_cfg['foreign_key_id'],
                s=fk_cfg['source_field_to_display'],
                t=fk_cfg['foreign_table']
            )
            fk = fk_cfg['fk_on_main_table']
            ix = self.get_field_index(fk)
            results = sqlite_pull(self.db, sql)
            self._foreign_keys[ix] = {
                row[0]: row[1]
                for row in results
            }
        return self._foreign_keys

    @property
    def headers(self) -> List[str]:
        """Return a list of field headers for display."""
        if self.fields:
            return [fld.name for fld in self.fields.values()]
        return []

    @immutable_property
    def main_table_fields(self) -> List[Tuple[int, str]]:
        # if self._main_table_fields:
        #     return self._main_table_fields
        # self._main_table_fields = inspect_table(db=self.db, table=self.table)
        # return self._main_table_fields
        return inspect_table(db=self.db, table=self.table)

    @property
    def max_display_rows(self) -> int:
        return self.config.get('max_display_rows', 0)

    @property
    def max_display_rows_clause(self) -> str:
        if self.max_display_rows > 0:
            return "LIMIT {}".format(self.max_display_rows)
        return ""

    @property
    def max_export_rows(self) -> int:
        return self.config.get('max_export_rows', 0)

    @property
    def max_export_rows_clause(self) -> str:
        if self.max_export_rows > 0:
            return "LIMIT {}".format(self.max_export_rows)
        return ""

    @property
    def order_by(self) -> str:
        if self.config['order_by']:
            return '{} {}'.format(self.config.get('order_by'), 'asc')
        return ''

    @property
    def order_by_clause(self) -> str:
        if self.order_by:
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
        return self.config['primary_key']

    @property
    def primary_key_index(self):
        return self.get_field_index(self.primary_key)

    @property
    def primary_table(self):
        return self.config['primary_table']

    @property
    def primary_table_fields(self):
        if self._primary_table_fields:
            return self._primary_table_fields
        return inspect_table(self.db, self.primary_table)

    @log_error
    @QtCore.pyqtSlot(list)
    def process_results(self, results: list) -> None:
        """Mutate the list in place"""
        try:
            for rownum, row in enumerate(results):
                for colnum, field in enumerate(row):
                    if colnum in self.foreign_keys.keys():
                        results[rownum][colnum] = self.foreign_keys[colnum].get(field)
                    elif self.fields[colnum].type == 'float':
                        results[rownum][colnum] = field or 0.0
                    else:
                        results[rownum][colnum] = field or ''
            self.query_results_signal.emit(results)
        except Exception as e:
            err_msg = "Error processing results: {}".format(e)
            self.error_signal.emit(err_msg)

    def reset(self) -> None:
        self._criteria = {}

    def reverse_fk_lookup(self, ix: int, val: str) -> int:
        """Iterate through foreign key and return first matching key for value"""
        return next((
            key for key, value
            in self.foreign_keys[ix].items()
            if value == val
        ), 0)

    @log_error
    def save_changes(self, changes: Dict[str, List[tuple]]) -> Dict[str, int]:
        """Persist a change to the database.

        Args:
            changes (dict): A dictionary where the key is the type of change,
                and the values is a tuple representing the row. The first field
                in the tuple must be an integer primary key for the table
                following: 'deleted', 'inserted', 'updated'

        Returns:
            str:  Empty string if successful, otherwise an error message.

        """
        rows_added = 0
        rows_deleted = 0
        rows_updated = 0
        try:
            for row in changes['deleted']:
                self.delete_row(row[self.primary_key_index])
                rows_deleted += 1

            for row in changes['added']:
                id = self.add_row(row)
                rows_added += 1

            for row in changes['updated']:
                id = self.update_row(row)
                rows_updated += 1

            return {
                'rows_added': rows_added
                , 'rows_deleted': rows_deleted
                , 'rows_updated': rows_updated
                , 'rows_errored': 0
            }

        except Exception as e:
            self.error_signal.emit("Error saving changes: {}".format(e))
            return {}



    @property
    def table(self) -> str:
        """Returns table name or a sql statement depending on config

        If it's a sql statement wrap it in parenthesis since it will be used
        as a subquery in the final output.
        """
        if re.match('.*[.]sql$', self.config.get('table')):
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
            , self.order_by_clause
            , self.max_display_rows_clause
        ))

    @property
    def sql_export(self) -> str:
        return ' '.join((
            self.select_statement
            , self.where_clause
            , self.order_by_clause
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

    def update_row(self, row: tuple) -> int:
        """Append row to database and return id value of insert"""
        main_table_fields = (fld.name for fld in self.main_table_fields)
        row_dict = {
            self.fields[i].name: val
            for i, val
            in enumerate(row)
            if self.fields[i].name in main_table_fields
                and i != self.primary_key_index
        }

        for i in self.foreign_keys.keys():
            name = self.fields[i].name
            row_dict[name] = self.reverse_fk_lookup(ix=i, val=row_dict[name])

        flds = [fld + " = :" + fld for fld in row_dict.keys()]
        id = row[self.primary_key_index]
        sql = ("UPDATE {t} SET " + ", ".join(flds) + " WHERE {p} = {i}")\
            .format(t=self.table, p=self.primary_key, i=id)
        with SQLiteConnection(self.db, read_only=False) as con:
            cur = con.cursor()
            cur.execute(sql, row_dict)
            return id

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

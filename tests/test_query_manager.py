from collections import namedtuple
from hypothesis import (
    assume
    , example
    , find
    , given
    , note
    , settings
    , Verbosity
)
from hypothesis.stateful import RuleBasedStateMachine, Bundle, rule, precondition
from hypothesis.strategies import (
    builds
    , booleans
    , dictionaries
    , floats
    , integers
    , lists
    , sampled_from
    , text
    , tuples
)
import pytest
import random
import re
import sqlite3

from query_manager import (
    Field
    , Operator
    , operator_options
    , QueryManager
    , where_condition
)
from utilities import valid_sql_field_name

FieldStrategy = builds(
    Field
    , name=text(alphabet="abcdefg", min_size=1)
    , type=sampled_from(['int', 'str', 'date'])
    , filterable=booleans()
)


@pytest.fixture(scope='module')
def example_fields():
    return {
        0: Field(name='first_name', type='str', filterable=True)
        , 1: Field(name='last_name', type='str', filterable=False)
        , 2: Field(name='order_date', type='date', filterable=True)
        , 3: Field(name='payment_date', type='date', filterable=False)
        , 4: Field(name='customer_id', type='int', filterable=True)
        , 5: Field(name='order_id', type='int', filterable=False)
    }


@pytest.fixture(scope='module')
def example_query_manager():
    config = {
        'db': 'test.db'
        , 'fields': []
        , 'table': 'Customers'
        , 'max_export_rows': 500000
        , 'max_display_rows': 1000
        , 'order_by': ''
    }
    return QueryManager(config)


def random_operator(field: Field) -> str:
    opts = operator_options(field).options
    try:
        return random.choice(opts)
    except:
        return ''


@given(field=FieldStrategy, value=text(min_size=1))
def test_add_criteria(field, value):
    random_op = random_operator(field)
    print('random op: {}'.format(random_op))
    qm = example_query_manager()
    qm.add_criteria(
        field=field
        , value=value
        , operator=random_op
    )
    assume(field.filterable)
    sql = qm.sql_display
    print(sql)
    note('sql: {}'.format(sql))
    valid_sql(qm.sql_display)


@given(FieldStrategy)
def test_operator_options(field):
    op = operator_options(field)
    assert isinstance(op.options, list)
    assert isinstance(op.default, str)
    if field.filterable:
        assert op.default
    else:
        assert op.default == ''


@given(FieldStrategy, text())
def test_valid_where_condition(field, s):
    opt = random_operator(field)
    where = where_condition(field=field, value=s, operator=opt)[0]
    sql = "SELECT {n} FROM tbl WHERE {w}".format(n=field.name, w=where)
    assume(where)
    note('operator:{}'.format(opt))
    note('where condition: {}'.format(where))
    note('sql:{}'.format(sql))
    assert valid_sql(stmt=sql)


# SPECIFIC EXAMPLES
def test_where_str_like(example_fields):
    where = where_condition(example_fields[0], value='Mark', operator='Like')[0]
    assert where == "first_name LIKE '%Mark%'"


def test_where_date_same_day(example_fields):
    where = where_condition(example_fields[2], value='2010-01-01', operator='Same day')[0]
    assert where == "order_date < date('2010-01-01', '+1 day') " \
                    "AND order_date > date('2010-01-01', '-1 day')"


def test_where_str_starts_with(example_fields):
    where = where_condition(example_fields[0], value='Mark', operator='Starts with')[0]
    assert where == "first_name LIKE 'Mark%'"


def test_where_str_ends_with(example_fields):
    where = where_condition(example_fields[0], value='Mark', operator='Ends with')[0]
    assert where == "first_name LIKE '%Mark'"


def valid_sql(stmt: str) -> bool:
    sql_parts = re.search(r"^(?:SELECT )(.*)(?: FROM )(\w*)(?:.*)$", stmt)
    fields = sql_parts.group(1)
    tbl = sql_parts.group(2)
    try:
        with sqlite3.connect(':memory:') as con:
            con.execute("CREATE TABLE {t} ({f})".format(t=tbl, f=fields))
            con.commit()
            con.execute(stmt)
            return True
    except Exception as e:
        note('stmt failed: {}'.format(stmt))
        note('field list: {}'.format(fields))
        note('error: {}'.format(str(e)))
        return False


class QueryManagerMachine(RuleBasedStateMachine):
    """Stateful tests for the Query Manager class"""

    fields = Bundle('fields')
    criteria = Bundle('criteria')

    def __init__(self):
        super(QueryManagerMachine, self).__init__()
        self.config = {
            'db': 'test.db'
            , 'fields': []
            , 'table': 'Customers'
            , 'max_export_rows': 500000
            , 'max_display_rows': 1000
            , 'order_by': ''
        }
        self.query_manager = QueryManager(self.config)


    @rule(target=fields, field=FieldStrategy)
    def add_field(self, field):
        assume(field.name not in self.query_manager.headers) # unique constraint
        assume(valid_sql_field_name(field.name))
        self.config.get('fields').append([
             field.name, field.type, field.filterable
        ])

    @rule(field=fields, value=text())
    def add_criteria(self, field, value):
        assume(field)
        assume(field.name)
        random_op = random_operator(field)
        self.query_manager.add_criteria(
            field=field
            , value=value
            , operator=random_op
        )

    @rule()
    def query_is_valid(self):
        assume(self.query_manager.fields)
        fieldlist = [
            fld.name
            for fld
            in self.query_manager.fields.values()
        ]
        note('field list: {}'.format(list(fieldlist)))
        note('sql display: {}'.format(self.query_manager.sql_display))
        assert valid_sql(stmt=self.query_manager.sql_display)

TestQueryManager = QueryManagerMachine.TestCase


if __name__ == '__main__':
    # pytest.main('-v ' + __file__)
    pytest.main(__file__)

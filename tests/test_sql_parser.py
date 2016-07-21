import pytest

from sql_parser import SqlParse, parse_sql


@pytest.fixture(scope='module')
def complex_sql():
    return """
        WITH Chease AS(
                SELECT
                    ID
                    , first_name AS FirstName
                    , LastName
                    , MIDDLE_INITIAL AS MiddleInitial
                    , City -- test comment
                    , SUM(Income) AS Income
                FROM dbo.MyTable AS t1
                LEFT OUTER JOIN dbo.Table2
                    ON t1.ID = t2.ID
                    AND t1.first_name = t2.first_name
                    and t1.first_name LIKE MIDDLE_INITIAL + '%'
                WHERE t1.ID = 1
                    AND t2.first_name like 'A%'
                GROUP BY
                    ID
                    , first_name
                    , LastName
                    , MIDDLE_INITIAL
                    , City
                /* multi line
                comment test */
        )
        SELECT first_name FROM Cheese
    """


@pytest.fixture(scope='module')
def simple_sql():
    return """
        SELECT Name, Price
        FROM fruits
        WHERE seedless = True
    """


@pytest.fixture(scope='module')
def complex_parse(complex_sql):
    return parse_sql(complex_sql)


@pytest.fixture(scope='module')
def simple_parse(simple_sql):
    return parse_sql(simple_sql)


@pytest.fixture(scope='module')
def complex_sqlparse_obj(complex_sql):
    return SqlParse(complex_sql)


@pytest.fixture(scope='module')
def simple_sqlparse_obj(simple_sql):
    return SqlParse(simple_sql)


def test_parse_sql_cte(complex_parse, simple_parse):
    assert complex_parse.get('cte') == {0: 'chease'}
    assert not simple_parse.get('cte')


def test_parse_sql_selected_items(complex_parse, simple_parse):
    assert complex_parse['selected_items'] == {
        0: 'id, first_name as firstname, lastname, middle_initial as '
            'middleinitial, city, sum(income) as income',
        1: 'first_name'
    }
    assert simple_parse['selected_items'] == {0: 'name, price'}


def test_parse_sql_main_table(complex_parse, simple_parse):
    assert complex_parse['main_table'] == {
        0: 'dbo.mytable as t1',
        1: 'cheese'
    }
    assert simple_parse['main_table'] == {0: 'fruits'}


def test_sql_parse_get_elements(complex_sqlparse_obj, simple_sqlparse_obj):
    assert complex_sqlparse_obj.get_subquery_elements(0).get('main_table') == 'dbo.mytable as t1'
    assert complex_sqlparse_obj.get_subquery_elements(1).get('main_table') == 'cheese'
    assert simple_sqlparse_obj.get_subquery_elements(0).get('main_table') == 'fruits'
    assert simple_sqlparse_obj.get_subquery_elements(1).get('main_table') == None


def test_sql_parse_subquery_count(complex_sqlparse_obj, simple_sqlparse_obj):
    assert complex_sqlparse_obj.subquery_count == 2
    assert simple_sqlparse_obj.subquery_count == 1


if __name__ == '__main__':
    print('complex parse:', complex_parse(complex_sql()))
    print('simple parse:', simple_parse(simple_sql()))
    pytest.main(__file__)


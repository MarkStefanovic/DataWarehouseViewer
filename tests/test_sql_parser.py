import pytest

from sql_parser import parse_sql


@pytest.fixture(scope='module')
def complex_parse():
    sql = """
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
    return parse_sql(sql)


@pytest.fixture(scope='module')
def simple_parse():
    sql = """
        SELECT Name, Price
        FROM fruits
        WHERE seedless = True
    """
    return parse_sql(sql)


def test_parse_sql_selected_items_complex(complex_parse):
    assert complex_parse['selected_items'] == (
        (0, 'id, first_name as firstname, lastname, middle_initial as '
            'middleinitial, city, sum(income) as income'),
        (1, 'first_name')
    )


def test_parse_sql_selected_items_simple(simple_parse):
    assert simple_parse['selected_items'] == (0, 'name, price')


def test_parse_sql_main_table_complex(complex_parse):
    assert complex_parse['main_table'] == (
        (0, 'dbo.mytable as t1'),
        (1, 'cheese')
    )


def test_parse_sql_main_table_simple(simple_parse):
    assert simple_parse['main_table'] == (0, 'fruits')

    # print('results dict:', parsed)
    # print('selected items:', parsed.get('selected_items')
if __name__ == '__main__':
    print('complex parse:', complex_parse())
    print('simple parse:', simple_parse())
    pytest.main(__file__)


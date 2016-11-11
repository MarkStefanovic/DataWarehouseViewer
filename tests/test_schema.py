from schema.calculated_field import (
    evaluate_expression,
    parse_expression
)

from pytest import (
    fixture,
    main
)


@fixture(scope='module')
def numeric_example():
    return "((([2] * [3]) - [2]) - [5]) / ([2] + [-1])"


@fixture(scope='module')
def text_example():
    return "((([apple] * [tree]) - [apple]) - [pear]) / ([tree] + [apple])"


@fixture(scope='module')
def mixed_example():
    return "((([apple] * [tree]) - [apple]) - [pear]) / ([tree] * [-1])"


def test_parse_numeric_expression(numeric_example):
    assert parse_expression(numeric_example) == \
           [[[['2', '*', '3'], '-', '2'], '-', '5'], '/', ['2', '+', '-1']]


def test_parse_text_expression(text_example):
    assert parse_expression(text_example) == \
           [[[['apple', '*', 'tree'], '-', 'apple'], '-', 'pear'], '/',
               ['tree', '+', 'apple']]


def test_parse_mixed_expression(mixed_example):
    assert parse_expression(mixed_example) == \
           [[[['apple', '*', 'tree'], '-', 'apple'], '-', 'pear'], '/',
               ['tree', '*', '-1']]


def test_evaluate_text_expression(text_example):
    lkp_table = {
        'apple': 10,
        'tree': 5,
        'pear': 4
    }
    assert evaluate_expression(text_example, lkp_table) == 2.4


def test_evaluate_numeric_expression(numeric_example):
    assert round(evaluate_expression(numeric_example, None), 2) == -1.0


def test_evaluate_numeric_expression(mixed_example):
    lkp_table = {
        'apple': 10,
        'tree': 5,
        'pear': 4
    }
    assert round(evaluate_expression(mixed_example, lkp_table), 2) == -7.2


if __name__ == '__main__':
    main(__file__)

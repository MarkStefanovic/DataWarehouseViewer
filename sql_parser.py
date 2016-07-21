"""This module contains tools to break a sql statement down into its constituent parts"""
from collections import defaultdict
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyparsing import (
    alphas
    , alphanums
    , Combine
    , Group
    , Literal
    , MatchFirst
    , NotAny
    , nums
    , OneOrMore
    , Optional
    , Suppress
    , White
    , Word
    , ZeroOrMore
)

SQL_KEYWORDS = {
    'and', 'select', 'with', 'as', 'from', 'exists', 'not', 'null', 'in'
    , 'substr', 'case', 'when', 'where', 'then', 'end', 'decimal',
    'varchar', 'nvarchar', 'int', 'float', 'like'
}

SQL_AGGREGATES = {'sum', 'avg', 'min', 'max'}


def clean(sql: str) -> str:
    clean_str = sql.strip()
    clean_str = re.sub('--+.+[\n]', ' ',
        clean_str)  # remove all single line comments
    clean_str = clean_str.lower()  # convert all text to lowercase
    clean_str = clean_str.replace('\t', ' ')  # remove all tabs
    clean_str = clean_str.replace('\n', ' ')  # remove all newline characters
    # clean_str = re.sub(r'\/\*.+\*.', ' ', clean_str)  # remove all multi-line comments
    clean_str = re.sub(' +', ' ', clean_str)  # remove all double spaces
    clean_str = re.sub(' ,', ', ', clean_str)  # remove space before commas
    clean_str = re.sub('\( ', ' (',
        clean_str)  # remove space before beginning parenthesis
    clean_str = re.sub(' \)', '\) ',
        clean_str)  # remove space before ending parenthesis
    clean_str = re.sub('  ', ' ', clean_str)  # remove double spaces
    return clean_str


def title_case(sql: str, exceptions: List[str]=[]) -> str:
    word_list = re.split(' ', sql)  # re.split behaves as expected
    final = [word_list[0].capitalize()]
    for word in word_list[1:]:
        final.append(word in exceptions and word or word.capitalize())
    return " ".join(final)


def capitalize_keywords(str):
    for word in SQL_KEYWORDS:
        str = re.sub('( |^)' + word + ' ', ' ' + word.upper() + ' ', str)
    for word in SQL_AGGREGATES:
        str = re.sub(' ' + word + '[(]', ' ' + word.upper() + '(', str)
    return str


def parse_sql(sql: str) -> Dict[str, Any]:
    clean_sql = clean(sql)

    comparison_operator = Combine(
        Optional(White())
        + MatchFirst([
            '!='
            , '<>'
            , '>='
            , '<='
            , '='
            , '>'
            , '<'
            , 'like'
        ])
        + Optional(White())
    )

    full_name = Combine(
        Word(alphanums)
        + ZeroOrMore(".")
        + ZeroOrMore(Word(alphanums))
        + ZeroOrMore("_")
        + ZeroOrMore(Word(alphanums))
        + ZeroOrMore(Word(alphanums))
        + ZeroOrMore("_")
        + ZeroOrMore(Word(alphanums))
    )

    alias = Combine(
        Optional(" as ")
        + full_name
    )

    full_name_with_alias = Combine(
        full_name
        + Optional(alias)
    )

    cte_declaration = Combine(
        Suppress(Literal("with "))
        + full_name
        + Suppress(
            Literal(" as")
            + Optional(White())
            + Literal("(")
        )
    )

    p_aggregate = Combine(
        MatchFirst([
            Literal("sum(")
            , Literal("min(")
            , Literal("max(")
            , Literal("avg(")
        ])
        + Optional(White())
        + full_name
        + Optional(White())
        + Literal(")")
    )

    p_select_fullname_with_alias = Combine(
        MatchFirst([
            p_aggregate
            , full_name
        ])
        + Optional(alias)
    )

    p_select_comma_seperated_list = Combine(
        p_select_fullname_with_alias
        + ZeroOrMore(
            Literal(",") + Optional(White())
            + p_select_fullname_with_alias
        )
    )

    assignment = Group(
        full_name
        + comparison_operator
        + full_name
    )

    join = MatchFirst([
        'full join '
        , 'left join '
        , 'right join '
        , 'full join '
        , 'left outer join '
        , 'right outer join '
    ])

    and_seperated_assignment = ZeroOrMore(
        full_name + comparison_operator + full_name
        + ZeroOrMore(
            " and "
            + full_name
            + comparison_operator
            + full_name
        )
    )

    cte_parser = cte_declaration.setResultsName('cte')

    selected_items_parser = Combine(
        Suppress("select ")  # anchor
        + p_select_comma_seperated_list
    ).setResultsName('selected_items')

    main_table_parser = Combine(
        Suppress("from ")  # anchor
        + full_name_with_alias
    ).setResultsName('main_table')

    join_condition_parser = Combine(
        join
        + full_name_with_alias
        + " on "
        + and_seperated_assignment
    ).setResultsName('join_conditions')

    where_condition_parser = Combine(
        Suppress("where ")
        + assignment
        + ZeroOrMore(" and " + assignment)
    ).setResultsName('where_conditions')

    parsers = {
        'cte':                  cte_parser
        , 'selected_items':     selected_items_parser
        , 'main_table':         main_table_parser
        , 'join_conditions':    join_condition_parser
        , 'where_conditions':   where_condition_parser
    }

    results = defaultdict(dict)
    for key, val in parsers.items():
        parsed_val = [
            match[0]
            for match, start, stop
            in val.scanString(clean_sql)
        ]
        for i, v in enumerate(parsed_val):
            results[key][i] = v
    return results


class SqlParse:
    def __init__(self, sql: str) -> None:
        self.elements = parse_sql(sql)

    def get_subquery_elements(self, ix: int) -> Dict[str, Any]:
        subquery = defaultdict(str)
        for key, val in self.elements.items():
            subquery[key] = val.get(ix)
        return subquery

    @property
    def subquery_count(self):
        return max(
            max(x for x in key.keys())
            for key
            in (k for k in self.elements.values())
        ) + 1

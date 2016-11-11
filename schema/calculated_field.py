import operator

from pyparsing import (
    alphanums,
    Combine,
    Forward,
    Group,
    Literal,
    oneOf,
    Optional,
    Word,
    ZeroOrMore
)
from sqlalchemy import Float
from sqlalchemy.sql.elements import BinaryExpression
from typing import List, Any, Dict

from schema.custom_types import FieldName, FieldType, FieldFormat
from schema.utilities import static_property, autorepr


def expression_syntax() -> Forward:
    """Syntax for a calculated sql expression

    :return: Forward
    """
    op = oneOf('+ - / *')
    lpar = Literal('(').suppress()
    rpar = Literal(')').suppress()
    field_name = Combine(
        Optional(Literal('-')) + Word(alphanums)
        + ZeroOrMore(oneOf(['_', ' ']) + Word(alphanums))
    )
    field = Literal('[').suppress() + field_name + Literal(']').suppress()
    expr = Forward()
    atom = field | Group(lpar + expr + rpar)
    expr << atom + ZeroOrMore(op + expr)
    return expr


def parse_expression(expr: str) -> List[Any]:
    """Break a string expression into its evaluatable parts

    :type expr: str
    :param expr: the expression to parse

    :rtype: List[Any]
    :return: expression broken down into sub-expressions
    """
    return (
        expression_syntax()
        .parseString(expr)
        .asList()
    )

operator_lkp = {
    '-': lambda v1, v2: v1 - v2,
    '*': lambda v1, v2: v1 * v2,
    '/': lambda v1, v2: v1 / v2,
    '+': lambda v1, v2: v1 + v2
}


def evaluate_field(fld: str, lkp: Dict):
    if isinstance(fld, BinaryExpression):
        return fld
    try:
        return float(fld)
    except ValueError:
        try:
            field = lkp[fld]
            try:
                return field.cast(Float(19, 2))
            except:
                return float(field)
        except KeyError:
            return fld


def resolve_branch(branch, lkp_table):
    fld_1, op, fld_2 = branch
    return operator_lkp[op](
        evaluate_field(fld_1, lkp_table),
        evaluate_field(fld_2, lkp_table)
    )


def recursively_evaluate(
        expr: List[Any],
        lkp_table: [Dict]
):
    """Recursively calculate the parsed expression"""
    fld_1, op, fld_2 = expr
    if isinstance(fld_1, list):
        print('fld_1 is a list')
        fld_1 = resolve_branch(fld_1, lkp_table) #recursively_evaluate(fld_1, lkp_table)
    if isinstance(fld_2, list):
        print('fld_2 is a list {}', fld_2)
        fld_2 = resolve_branch(fld_2, lkp_table) #recursively_evaluate(fld_2, lkp_table)
    print('operator:', operator_lkp[op])
    val = operator_lkp[op](
        evaluate_field(fld_1, lkp_table),
        evaluate_field(fld_2, lkp_table)
    )
    print('val:', val)
    return val


def evaluate_expression(
        expr: str,
        lkp_table: [Dict]
) -> Any:
    """Break a string expression into parts and evaluate

    :type expr: str
    :param expr: string expression to evaluate

    :rtype: Any
    :return: evaluated expression
    """
    parsed_expression = parse_expression(expr)
    print('parsed_expression:', parsed_expression)
    return recursively_evaluate(
        expr=parsed_expression,
        lkp_table=lkp_table,
    )


@autorepr
class CalculatedField:
    """A field that represents the combination of one or more fields in a Star.

    It mimics its base field except for the schema and editability"""
    def __init__(self, *,
        formula: str,
        display_name: FieldName,
        show_on_fact_table: bool=True
    ) -> None:

        self.formula = formula
        self.display_name = display_name
        self.show_on_fact_table = show_on_fact_table
        # The star property is injected by the Star itself later.
        # It must be populated before this field can be used.
        self._star = None

        self.validate_config()

    @static_property
    def dtype(self):
        """Mimic field property"""
        return FieldType.Float
        # dtypes = {
        #     'count': FieldType.Int,
        #     'avg': FieldType.Float,
        #     'sum': FieldType.Float
        # }
        # try:
        #     return dtypes[self.sqa_func]
        # except KeyError:
        #     print('Unable to find data type of AdditiveField {}'
        #           .format(self.display_name))
        #     return self.base_field.dtype

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
        return self.display_name

    @static_property
    def schema(self) -> BinaryExpression:
        try:
            return evaluate_expression(self.formula, self.star.base_field_schema_lkp).label(self.display_name)
        except Exception as e:
            print('error creating schema for calculated field {}; error: {}')

    @static_property
    def primary_key(self):
        """Mimic field property"""
        return False

    # @static_property
    # def sqa_dtype(self):
    #     lkp = {
    #         'avg': Float(14, 2),
    #         'count': Integer,
    #         'sum': Float(14, 2)
    #     }
    #     try:
    #         return lkp[self.sqa_func]
    #     except KeyError:
    #         print('Unable to find sqa_dtype for AdditiveField {} sqa_func {}'
    #               .format(self.display_name, self.sqa_func))

    @static_property
    def star(self):
        if not self._star:
            raise(AttributeError, "The star for CalculatedField {} was not"
                                  "injected prior to calling the field."
                                  .format(self.display_name))
        return self._star

    def validate_config(self):
        from schema.config import ConfigError
        if not self.formula:
            raise ConfigError("The formula for CalculatedField {} is blank"
                              .format(self.display_name))

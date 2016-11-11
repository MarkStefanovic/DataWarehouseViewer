import sqlalchemy as sqa
from sqlalchemy.sql.elements import BinaryExpression
from typing import Optional

from schema.custom_types import Operator, SqlDataType
from schema.field import Field
from schema.munge import convert_value
from schema.utilities import autorepr, static_property


@autorepr
class FilterConfig:
    """This class serves as a temporary holding tank for would-be Filter configs"""
    def __init__(self, *,
            operator: Operator,
            default_value: Optional[SqlDataType]=''
    ) -> None:
        self.default_value = default_value
        self.operator = operator


@autorepr
class Filter:
    def __init__(self, *,
            field: Field,
            operator: Operator
    ) -> None:

        self.field = field
        self.operator = operator
        self._value = None  # type: Optional[SqlDataType]

    @static_property
    def display_name(self) -> str:
        suffix = self.operator.suffix
        return self.field.display_name + (" " + suffix if suffix else "")

    @property
    def filter(self) -> BinaryExpression:
        fld = self.field.schema
        operator_mapping = {
            Operator.number_equals: fld == self.value,
            Operator.number_does_not_equal: fld != self.value,
            Operator.number_greater_than: fld > self.value,
            Operator.number_greater_than_or_equal_to: fld >= self.value,
            Operator.number_less_than: fld < self.value,
            Operator.number_less_than_or_equal_to: fld <= self.value,

            Operator.str_equals: fld == self.value,
            Operator.str_like: fld.contains(self.value),
            Operator.str_not_like: fld.notlike('%{}%'.format(self.value)),
            Operator.str_starts_with: fld.startswith(self.value),
            Operator.str_ends_with: fld.endswith(self.value),

            Operator.date_after: sqa.func.date(fld) > self.value,
            Operator.date_on_or_after: sqa.func.date(fld) >= self.value,
            Operator.date_before: sqa.func.date(fld) < self.value,
            Operator.date_on_or_before: sqa.func.date(fld) <= self.value,
            Operator.date_equals: sqa.func.date(fld) == self.value,
            Operator.date_does_not_equal: sqa.func.date(fld) != self.value
        }
        if self.value:
            return operator_mapping[self.operator]

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

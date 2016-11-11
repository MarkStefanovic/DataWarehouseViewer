from typing import Optional, List

import sqlalchemy as sqa

from schema.utilities import autorepr, static_property
from schema.custom_types import FieldType, FieldFormat, Operator


@autorepr
class Field:
    """Instances of this class represent a column in a database table."""
    def __init__(self, *,
            name: str,
            dtype: FieldType,
            display_name: str,
            field_format: Optional[FieldFormat]=None,
            filter_operators: Optional[List[Operator]]=None,
            editable: bool=True,
            primary_key: bool=False
    ) -> None:

        self.name = name
        self.dtype = dtype
        self.display_name = display_name
        self.field_format = field_format
        self.editable = editable
        self.primary_key = primary_key
        self.filter_operators = filter_operators

    @static_property
    def schema(self) -> sqa.Column:
        """Map the field to a sqlalchemy Column"""
        type_map = {
            FieldType.Bool:  sqa.Boolean,
            FieldType.Date:  sqa.Date,
            FieldType.Float: sqa.Float,
            FieldType.Int:   sqa.Integer,
            FieldType.Str:   sqa.String
        }
        return sqa.Column(
            self.name,
            type_=type_map[self.dtype](),
            primary_key=self.primary_key
        )

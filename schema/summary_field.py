from typing import List, Optional

from schema.custom_types import Operator, FieldType, FieldFormat
from schema.field import Field
from schema.utilities import autorepr


@autorepr
class SummaryField(Field):
    """Concatenate multiple fields

    This field type is used for display on associated fact tables in lieu of
    their integer primary key.
    """
    def __init__(self, *,
            display_fields: List[str],
            display_name: str,
            separator: str = ' ',
            filter_operators: Optional[List[Operator]] = None
    ) -> None:
        super(SummaryField, self).__init__(
            name="_".join(display_fields),
            dtype=FieldType.Str,
            display_name=display_name,
            field_format=FieldFormat.Str,
            filter_operators=filter_operators,
            editable=False,
            primary_key=False
        )

        self.display_fields = display_fields
        self.display_name = display_name
        self.separator = separator



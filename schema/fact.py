from typing import List, Optional

from schema.calculated_field import CalculatedField
from schema.custom_types import FactName, OrderBy, DimensionName
from schema.field import Field
from schema.filter import Filter
from schema.table import Table
from schema.utilities import autorepr


@autorepr
class Fact(Table):
    """Fact table specification

    Fact tables are generally long, but not wide.  They primarily contain data
    that is aggregated, and references to dimension tables for contextual data.
    They may also contain 'junk' dimensions, which are simply dimensions that
    don't warrant a separate table to store them.
    """

    def __init__(self, *,
            table_name: FactName,
            display_name: str,
            fields: List[Field],
            show_on_load: bool=False,
            editable: bool=False,
            display_rows: int=10000,
            order_by: Optional[List[OrderBy]]=None,
            calculated_fields: Optional[List[CalculatedField]]=None
    ) -> None:

        super(Fact, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            show_on_load=show_on_load,
            editable=editable,
            display_rows=display_rows,
            order_by=order_by
        )

        self.calculated_fields = calculated_fields

    @property
    def dimensions(self) -> List[DimensionName]:
        """List of the associated dimension names"""
        return [
            fld.dimension
            for fld in self.foreign_keys.values()
        ]

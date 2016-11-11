from functools import reduce

from sqlalchemy.sql import Select
from typing import List, Optional

import sqlalchemy as sqa

from schema.custom_types import DimensionName, OrderBy, FieldName, SortOrder, \
    FieldType, FieldFormat
from schema.field import Field
from schema.filter import Filter
from schema.summary_field import SummaryField
from schema.table import Table
from schema.utilities import autorepr, static_property


@autorepr
class Dimension(Table):
    """Dimension table specifications

    We don't specify the maximum display or export rows since dimension tables
    should be (and in this case *must* have a low row count, and the user must
    be able to see the entire dimension to edit any foreign keys that may show
    up on the associated Fact table.
    """

    def __init__(self, *,
            table_name: DimensionName,
            display_name: str,
            fields: List[Field],
            summary_field: SummaryField,
            editable: bool=False,
            show_on_load: bool=True,
            order_by: Optional[List[OrderBy]]=None,
            display_rows: int=10000
    ) -> None:

        super(Dimension, self).__init__(
            table_name=table_name,
            display_name=display_name,
            fields=fields,
            editable=editable,
            show_on_load=show_on_load,
            display_rows=display_rows,
            order_by=order_by
        )

        self.summary_field = summary_field

    @static_property
    def display_field_schemas(self) -> List[sqa.Column]:
        return [
            self.field(n).schema
            for n in self.summary_field.display_fields
        ]

    @static_property
    def foreign_key_schema(self) -> Table:
        summary_field = reduce(
            lambda x, y: x + self.summary_field.separator + y,
            self.display_field_schemas
        ).label(self.summary_field.display_name)
        return sqa.select([self.primary_key, summary_field])

    @static_property
    def order_by_schema(self):
        """The default sort order for the table"""

        def lkp_sort_order(
                fld_name: FieldName,
                sort_order: Optional[SortOrder]=None):

            fld = self.field(fld_name).schema
            if sort_order == SortOrder.Ascending:
                return fld.asc()
            return fld.desc()

        if self.order_by:
            return [
                lkp_sort_order(o.field_name, o.sort_order)
                for o in self.order_by
            ]

    @property
    def select(self, max_rows: int=1000) -> Select:
        """Only the dimension has a select method on the table class since
        the Fact table has to consider foreign keys so its select statement
        is composed at the Star level"""
        s = self.schema.select()
        for f in (flt for flt in self.filters if flt.value):
            s = s.where(f.filter)
        if self.order_by_schema:
            for o in self.order_by_schema:
                s = s.order_by(o)
        return s.limit(max_rows)

    @static_property
    def summary_field_schema(self) -> sqa.Column:
        fld = Field(
            name=self.summary_field.display_name,
            display_name=self.summary_field.display_name,
            dtype=FieldType.Str,
            field_format=FieldFormat.Str
        )
        fld.schema = reduce(
            lambda x, y: x + self.summary_field.separator + y,
            self.display_field_schemas).label(self.summary_field.display_name)
        return fld

import sqlalchemy as sqa
from sqlalchemy.sql import (
    Delete,
    Insert,
    Update
)
from typing import List, Optional, Dict

from schema import md
from schema.custom_types import (
    SortOrder,
    ColumnIndex,
    PrimaryKeyIndex,
    SqlDataType
)
from schema.field import Field
from schema.filter import Filter
from schema.foreign_key import ForeignKey
from schema.munge import convert_value
from schema.utilities import autorepr, static_property


@autorepr
class Table:
    """A container to store fields

    This class is meant to be subclassed by Dimension and Fact table classes.
    """

    def __init__(self,
            table_name: str,
            display_name: str,
            fields: List[Field],
            editable: bool,
            show_on_load: bool=False,
            order_by: Optional[List[SortOrder]]=None,
            display_rows: int=10000,
    ) -> None:

        self.table_name = table_name
        self.display_name = display_name
        self.fields = fields
        self.editable = editable
        self.display_rows = display_rows
        self.order_by = order_by
        self.show_on_load = show_on_load

    def add_row(self, values: List[str]) -> Insert:
        """Statement to add a row to the table given a list of values"""

        # we want to use the primary key assigned by the db rather
        # than the one we auto-generated as a placeholder
        values_sans_pk = {
            fld.name: convert_value(value=values[i],
                                    field_type=self.fields[i].dtype)
            for i, fld in enumerate(self.fields)
            if not fld.primary_key
        }
        return self.schema.insert().values(values_sans_pk)

    def delete_row(self, id: int) -> Delete:
        """Statement to delete a row from the table given the primary key value."""
        return self.schema.delete().where(self.primary_key == id)

    def field(self, name: str) -> Field:
        """Look up a field based on it's name on the table."""
        try:
            return next(fld for fld in self.fields if fld.name == name)
        except StopIteration:
            print('could not find table field named {} on table {}'
                  .format(name, self.table_name))

    @static_property
    def filters(self) -> List[Filter]:
        return [
            Filter(field=fld, operator=op)
            for fld in self.fields if fld.filter_operators
            for op in fld.filter_operators
        ]

    @static_property
    def foreign_keys(self) -> Dict[ColumnIndex, Field]:
        return {
            ColumnIndex(i): fld
            for i, fld in enumerate(self.fields)
            if isinstance(fld, ForeignKey)
        }

    @static_property
    def primary_key(self) -> Field:
        try:
            return next(c for c in self.schema.columns if c.primary_key is True)
        except StopIteration:
            print('could not find the primary key for table {}'
                  .format(self.table_name))

    @static_property
    def primary_key_index(self) -> PrimaryKeyIndex:
        try:
            return PrimaryKeyIndex(
                next(i for i, c in enumerate(self.schema.columns)
                     if c.primary_key)
            )
        except StopIteration:
            print('could not find the primary key index for table {}'
                  .format(self.table_name))

    @static_property
    def schema(self) -> sqa.Table:
        """Map table to a sqlalchemy table schema"""
        try:
            cols = [fld.schema for fld in self.fields]
            return sqa.Table(self.table_name, md, *cols)
        except Exception as e:
            print('Error creating the schema for table {}; error: {}'
                  .format(self.table_name, str(e)))

    def update_row(self, *,
            pk: PrimaryKeyIndex,
            values: List[SqlDataType]
    ) -> Update:
        """Statement to update a row on the table given the primary key value."""
        vals = {
            fld.name: convert_value(value=values[i],
                                    field_type=self.fields[i].dtype)
            for i, fld in enumerate(self.fields)
            if not fld.primary_key
        }
        return self.schema.update().where(self.primary_key == pk).values(vals)


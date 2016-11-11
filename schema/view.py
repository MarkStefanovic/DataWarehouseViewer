from sqlalchemy import Column
from sqlalchemy import select
from sqlalchemy.sql import Select
from typing import (
    Optional,
    List,
    Dict
)

from schema.additive_field import AdditiveField
from schema.custom_types import (
    FactName,
    FieldName,
    OrderBy,
    ColumnIndex,
    SortOrder
)
from schema.field import Field
from schema.filter import Filter
from schema.star import Star
from schema.utilities import autorepr, static_property


@autorepr
class View:
    """An aggregate view over a Star"""

    def __init__(self, *,
        view_display_name: str,
        fact_table_name: FactName,
        group_by_field_names: List[FieldName],
        additive_fields: Optional[List[AdditiveField]],
        order_by: Optional[List[OrderBy]] = None,
        show_on_load: bool=False
    ) -> None:

        self.display_name = view_display_name  # type: str
        self._fact_table_name = fact_table_name  # type: str
        self._group_by_fields = group_by_field_names  # type: Optional[List[FieldName]]
        self._additive_fields = additive_fields  # type: Optional[List[AdditiveField]]
        self.primary_key_index = -1
        self.editable = False
        self.show_on_load = show_on_load
        self.order_by = order_by

    @static_property
    def additive_fields(self) -> List[AdditiveField]:
        if self._additive_fields:
            for fld in self._additive_fields:
                fld._star = self.star
            return self._additive_fields
        return []

    @static_property
    def star(self) -> Star:
        """Get a reference to the Star associated with the current Fact table"""
        from schema.config import cfg
        return cfg.star(fact_table=self._fact_table_name)

    def field_by_display_name(self, display_name: FieldName) -> Field:
        """Lookup a Field by it's display name."""
        try:
            return next(fld
                        for fld in self.fields
                        if fld.display_name == display_name)
        except KeyError:
            print('Error looking up field_by_display_name; could not find a field'
                  'named {} in the View {}'.format(display_name, self.display_name))
        except Exception as e:
            print('Error looking up field_by_display_name: err {}'.format(str(e)))

    @property
    def filters(self) -> List[Filter]:
        return self.star.filters

    @static_property
    def foreign_keys(self) -> Dict[ColumnIndex, Field]:
        return {
            i: fld
            for i, fld in enumerate(self.fields)
            if fld.name in [
                f.name
                for f in self.star.fact.foreign_keys.values()
            ]
        }

    @static_property
    def group_by_fields(self):
        return [
            self.star.fields_by_display_name[fld_name]
            for fld_name in self._group_by_fields
        ]

    @static_property
    def fields_schema(self):
        return [fld.schema for fld in self.group_by_fields] \
               + [fld.schema for fld in self.additive_fields]

    @static_property
    def fields(self):
        return self.group_by_fields + self.additive_fields

    @static_property
    def order_by_schema(self) -> List[Column]:
        """Return the order by fields for the View"""
        def lkp_sort_order(order_by: OrderBy):
            try:
                fld = self.field_by_display_name(order_by.field_name)
                if order_by.sort_order == SortOrder.Ascending:
                    return fld.schema.asc()
                return fld.schema.desc()
            except KeyError:
                print('view.py: Unable to look up sort order for View {}, '
                      'field {}.'.format(self.display_name, order_by.field_name))

        if self.order_by:
            return [
                lkp_sort_order(o)
                for o in self.order_by
            ]
        return []

    @property
    def select(self) -> Select:
        try:
            star = self.star.fact.schema  # type: sqa.Table
            for dim in self.star.dimensions:
                star = star.outerjoin(dim.schema)
            qry = select(self.fields_schema).select_from(star)
            for f in [flt for flt in self.star.filters if flt.value]:
                qry = qry.where(f.filter)
            for g in self.group_by_fields:
                qry = qry.group_by(g.schema)
            if self.order_by_schema:
                for o in self.order_by_schema:
                    qry = qry.order_by(o)
            return qry.limit(self.star.display_rows)
        except Exception as e:
            print('view.py: Error composing select statement for View {}; '
                  'error {}'.format(self.display_name, str(e)))


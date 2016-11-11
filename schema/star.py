from collections import ChainMap
from itertools import chain

from sqlalchemy import select
from sqlalchemy.sql import Select
from typing import List, Iterable, Dict

from schema.custom_types import FieldName, OrderBy, SortOrder
from schema.dimension import Dimension
from schema.fact import Fact
from schema.field import Field
from schema.filter import Filter
from schema.foreign_key import ForeignKey
from schema.utilities import autorepr, static_property


@autorepr
class Star:
    """A Star is a Fact table with associated Dimensions

    A Star is a view for a fact table.  It inherits its editability
    from its core star.
    """
    def __init__(self, *,
        fact: Fact,
        dimensions: List[Dimension]=None
    ) -> None:

        self.fact = fact
        self._dimensions = dimensions

    @static_property
    def calculated_fields(self):
        if self.fact.calculated_fields:
            for fld in self.fact.calculated_fields:
                fld._star = self
            return [fld for fld in self.fact.calculated_fields]
        return []

    @static_property
    def dimensions(self) -> Iterable[Dimension]:
        return [
            dim for dim in self._dimensions
            if dim.table_name in [
                fld.dimension
                for fld in self.fact.fields
                if isinstance(fld, ForeignKey)
            ]
        ]

    @static_property
    def display_rows(self) -> int:
        return self.fact.display_rows

    @property
    def editable(self) -> bool:
        return self.fact.editable

    @static_property
    def fields(self) -> Dict[FieldName, Field]:
        """Fields list"""
        return self.fact.fields + self.calculated_fields

    @static_property
    def base_field_schema_lkp(self) -> Dict[FieldName, Field]:
        """Base fields indexed by their display name"""
        dim_fields = {}
        for dim in self.dimensions:
            for fld in dim.fields:
                dim_fields[fld.display_name] = fld.schema
        fact_fields = {
            fld.display_name: fld.schema
            for fld in self.fact.fields
        }
        return ChainMap({}, fact_fields, dim_fields)

    @static_property
    def filters(self) -> List[Filter]:
        star_filters = []  # type: List
        for dim in self.dimensions:
            for op in dim.summary_field.filter_operators:
                fk_filter = Filter(
                    field=dim.summary_field_schema,
                    operator=op
                )
                star_filters.append(fk_filter)
        for f in (flt for flt in self.fact.filters):
            star_filters.append(f)
        return sorted(star_filters)

    @static_property
    def fields_by_display_name(self) -> Dict[FieldName, Field]:
        # fact_fields = {fld.display_name: fld for fld in self.fact.fields}
        # all_fields = ChainMap({}, fact_fields,
        #                           self.summary_fields,
        #                           self.calculated_fields)
        # return all_fields
        return {
            fld.display_name: fld
            for fld in self.fields
        }

    @property
    def select(self) -> Select:
        """Override the Fact tables select method implementation to
        account for foreign key filters."""
        return self.star_query.limit(self.fact.display_rows)

    @static_property
    def summary_fields(self) -> Dict[FieldName, Field]:
        return {
            str(dim.summary_field.display_name): dim.summary_field_schema.schema
            for dim in self.dimensions
        }  # type: Dict[FieldName, Field]

    @static_property
    def order_by(self):
        return self.fact.order_by

    @static_property
    def order_by_schema(self):
        """Return the order by fields for the Star"""
        if not self.order_by:
            return

        def lkp_sort_order(order_by: OrderBy):
            if order_by.field_name in self.summary_fields.keys():
                fld = self.summary_fields[order_by.field_name]
            else:
                fld = self.fact.field(order_by.field_name).schema
            if order_by.sort_order == SortOrder.Ascending:
                return fld.asc()
            return fld.desc()

        return [
            lkp_sort_order(o)
            for o in self.order_by
        ]

    @property
    def star_query(self):
        try:
            calculated_fields = [
                fld.schema
                for fld in self.calculated_fields
            ]
            fact = self.fact.schema  # type: sqa.Table
            star = fact
            for dim in self.dimensions:
                star = star.outerjoin(dim.schema)
            if calculated_fields:
                fields = fact.columns + calculated_fields
            else:
                fields = fact.columns
            qry = select(fields).select_from(star)
            for f in [flt for flt in self.filters if flt.value]:
                qry = qry.where(f.filter)
            if self.order_by_schema:
                # noinspection PyTypeChecker
                for o in self.order_by_schema:
                    qry = qry.order_by(o)
            return qry
        except Exception as e:
            print('error composing star query: {}'.format(str(e)))

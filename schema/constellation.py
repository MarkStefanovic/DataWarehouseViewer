from itertools import chain

from sortedcollections import ValueSortedDict
from sqlalchemy.sql import Select
from typing import Optional, List, Dict

from schema.custom_types import FactName, DimensionName, ForeignKeyValue, \
    SqlDataType, ViewName
from schema.dimension import Dimension
from schema.fact import Fact
from schema.star import Star
from schema.table import Table
from schema.view import View
from schema.utilities import static_property


class Constellation:
    """Collection of all the Stars in the application"""

    def __init__(self, *,
            app,
            dimensions: Optional[List[Dimension]],
            facts: List[Fact],
            views: List[View]
    ) -> None:

        self.app = app
        self.dimensions = dimensions  # Optional[List[Dimension]]
        self.facts = facts  # type: List[Fact]
        self.views = views  # type Optional[List[View]]
        self._foreign_keys = {
            tbl.table_name: {}
            for tbl in dimensions
        }  # type: Dict[str, Dict[int, str]]

    @static_property
    def stars(self) -> Dict[FactName, Star]:
        return {
            fact.table_name: Star(fact=fact, dimensions=self.dimensions)
            for fact in self.facts
        }

    # @static_property
    # def views(self) -> Dict[ViewName, View]:
    #     return {
    #         view.display_name: view
    #         for view in self.views
    #     }

    @static_property
    def tables(self) -> List[Table]:
        return chain(self.facts, self.dimensions, self.views)

    @property
    def foreign_key_lookups(self) -> Dict[DimensionName, Select]:
        return {
            tbl.table_name: tbl.foreign_key_schema
            for tbl in self.dimensions
        }

    def foreign_keys(self, dim: DimensionName) -> Dict[ForeignKeyValue,
                                                       SqlDataType]:
        if self._foreign_keys[dim]:
            return self._foreign_keys[dim]
        self.pull_foreign_keys(dim)
        fks = self._foreign_keys[dim]
        if not 0 in fks:
            self._foreign_keys[dim][0] = ""
            fks[0] = ""
        # if not '' in fks:
        #     self._foreign_keys[dim][''] = ""
        #     fks[''] = ""
        return fks

    def pull_foreign_keys(self, dim: DimensionName) -> None:
        try:
            select_statement = self.foreign_key_lookups[dim]  # type: Select
            from schema.db import fetch
            self._foreign_keys[dim] = ValueSortedDict({
                row[0]: str(row[1])
                for row in fetch(select_statement)
            })
        except Exception as e:
            print('error pulling foreign keys for dimension {}'
                  .format(dim))

    def star(self, fact_table: FactName) -> Star:
        """Return the specific Star system localized on a specific Fact table"""
        try:
            return self.stars[fact_table]
        except KeyError:
            print('The fact table {} could not be found in the cfg global variable.'
                  .format(fact_table))

    def view(self, view_name: ViewName) -> View:
        """Return the specified View"""
        try:
            return next(view for view in self.views if view.display_name == view_name)
        except StopIteration:
            print('A view with the display name {} could not be found in the '
                  'cfg global variable.'.format(view_name))

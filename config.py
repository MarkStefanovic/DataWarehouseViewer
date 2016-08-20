from itertools import chain

from sortedcollections import ValueSortedDict
from typing import (
    Dict,
    Optional,
    List
)

from schema import (
    Dimension,
    Fact,
    Field,
    FieldFormat,
    FieldType,
    ForeignKey,
    Operator,
    SummaryField,
    Table,
)
from utilities import immutable_property

class App:
    def __init__(self, *,
        color_scheme: str,
        db_path: str,
        display_name: str,
        maximum_display_rows: int,
        maximum_export_rows: int
    ) -> None:

        self.color_scheme = color_scheme
        self.db_path = db_path
        self.display_name = display_name
        self.maximum_display_rows = maximum_display_rows
        self.maximum_export_rows = maximum_export_rows


class Config:
    def __init__(self, *,
        app: App,
        dimensions: Optional[List[Dimension]],
        facts: List[Fact]
    ) -> None:

        super(Config, self).__init__()

        self.app = app
        self.dimensions = dimensions
        self.facts = facts
        self._foreign_keys = {
            tbl.table_name: {}
            for tbl in dimensions
        }  # type Dict[str, Dict[int, str]]

    @immutable_property
    def tables(self) -> List[Table]:
        return chain(self.facts, self.dimensions)

    @property
    def foreign_key_lookups(self):
        return {
            tbl.table_name: tbl.foreign_key_schema
            for tbl in self.dimensions
        }

    def foreign_keys(self, dim: str) -> Dict[int, str]:
        if self._foreign_keys[dim]:
            return self._foreign_keys[dim]
        self.pull_foreign_keys(dim)
        return self._foreign_keys[dim]

    def pull_foreign_keys(self, dim: str) -> None:
        from db import fetch
        self._foreign_keys[dim] = ValueSortedDict({
            row[0]: str(row[1])
            for row in fetch(self.foreign_key_lookups[dim])
        })


cfg = Config(
    app=App(
        display_name='SalesDW'
        , color_scheme='darkcity.css'
        , db_path='sqlite:///test.db'
        , maximum_display_rows=1000
        , maximum_export_rows=500000
    ),
    dimensions=[
        Dimension(
            table_name='dimProduct',
            display_name='Products',
            editable=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='ProductName',
                    dtype=FieldType.str,
                    display_name='Name',
                    filter_operators=[Operator.str_like]
                ),
                Field(
                    name='ProductCategory',
                    dtype=FieldType.str,
                    display_name='Category',
                    filter_operators=[Operator.str_like]
                )
            ],
            summary_field=SummaryField(
                display_fields=['ProductName', 'ProductCategory'],
                display_name='Product',
                separator=' - '
            )
        )
        , Dimension(
            table_name='dimCustomer',
            display_name='Customers',
            editable=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='CustomerName',
                    dtype=FieldType.str,
                    display_name='Customer Name',
                    filter_operators=[Operator.str_like]
                ),
                Field(
                    name='ShippingAddress',
                    dtype=FieldType.str,
                    display_name='Shipping Address',
                    filter_operators=[Operator.str_like]
                ),
            ],
            summary_field=SummaryField(
                display_fields=['CustomerName'],
                display_name='Customer',
                separator=' - '
            )
        )
    ],
    facts=[
        Fact(
            table_name='factSales',
            display_name='Sales',
            editable=True,
            fields=[
                Field(
                    name='OrderID',
                    dtype=FieldType.int,
                    display_name='ID',
                    primary_key=True
                ),
                ForeignKey(
                    name='ProductID',
                    display_name='Product',
                    dimension='dimProduct'
                ),
                ForeignKey(
                    name='CustomerID',
                    display_name='Customer',
                    dimension='dimCustomer'
                ),
                Field(
                    name='OrderDate',
                    dtype=FieldType.date,
                    display_name='Order Date',
                    filter_operators=[
                        Operator.date_on_or_after,
                        Operator.date_on_or_before,
                    ]
                ),
                Field(
                    name='ShippingDate',
                    dtype=FieldType.date,
                    display_name='Shipping Date',
                    filter_operators=[
                        Operator.date_on_or_after,
                        Operator.date_on_or_before,
                    ]
                ),
                Field(
                    name='SalesAmount',
                    dtype=FieldType.float,
                    display_name='Sales Amount',
                    field_format=FieldFormat.currency,
                    filter_operators=[
                        Operator.number_equals
                    ]
                ),
            ]
        )
    ]
)

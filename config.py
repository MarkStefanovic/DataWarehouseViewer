from itertools import chain
from typing import (
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

        self.app = app
        self.dimensions = dimensions
        self.facts = facts

    @immutable_property
    def foreign_key_lookups(self):
        return {
            tbl.table_name: tbl.foreign_key_schema
            for tbl in self.dimensions
        }

    @immutable_property
    def tables(self) -> List[Table]:
        return chain(self.facts, self.dimensions)


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
            table_name='Products',
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
    ],
    facts=[
        Fact(
            table_name='SalesHistory',
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
                    display_name='Products',
                    dimension='Products'
                ),
                Field(
                    name='CustomerID',
                    dtype=FieldType.int,
                    display_name='Customer'
                ), # TODO make Customer Dimension to hook this into
                Field(
                    name='CustomerName',
                    dtype=FieldType.str,
                    display_name='Customer Name',
                    filter_operators=[Operator.str_like]
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
                    name='ShippingAddress',
                    dtype=FieldType.str,
                    display_name='Shipping Address',
                    filter_operators=[Operator.str_like]
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

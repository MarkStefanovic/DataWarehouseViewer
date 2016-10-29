from schema import (
    Constellation,
    Dimension,
    Fact,
    Field,
    FieldFormat,
    FieldType,
    ForeignKey,
    Operator,
    SummaryField
)


class App:
    def __init__(self, *,
        color_scheme: str,
        db_path: str,
        display_name: str
    ) -> None:

        self.color_scheme = color_scheme
        self.db_path = db_path
        self.display_name = display_name


cfg = Constellation(
    app=App(
        display_name='SalesDW',
        color_scheme='darkcity.css',
        db_path='sqlite:///test.db'
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
                separator=' - ',
                filter_operators=[Operator.str_like]
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
                separator=' - ',
                filter_operators=[Operator.str_like]
            )
        )
    ],
    facts=[
        Fact(
            table_name='factSales',
            display_name='Sales',
            editable=True,
            display_rows=10000,
            export_rows=500000,
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
                    dimension='dimProduct',
                    foreign_key_field='ID'
                ),
                ForeignKey(
                    name='CustomerID',
                    display_name='Customer',
                    dimension='dimCustomer',
                    foreign_key_field='ID'
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
                    field_format=FieldFormat.accounting,
                    filter_operators=[
                        Operator.number_equals
                    ]
                ),
                Field(
                    name='Paid',
                    dtype=FieldType.bool,
                    display_name='Paid?',
                    field_format=FieldFormat.str,
                    filter_operators=[
                    ]
                ),
            ]
        )
    ]
)

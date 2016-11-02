from schema import (
    Constellation,
    Dimension,
    Fact,
    Field,
    FieldFormat,
    FieldType,
    ForeignKey,
    Operator,
    SummaryField,
    OrderBy,
    SortOrder,
    View
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
            show_on_load=True,
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
            ),
            order_by=[
                OrderBy(
                    field_name='ProductName',
                    sort_order=SortOrder.ascending
                )
            ]
        )
        , Dimension(
            table_name='dimCustomer',
            display_name='Customers',
            editable=True,
            show_on_load=True,
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
            ),
            order_by=[
                OrderBy(
                    field_name='CustomerName',
                    sort_order=SortOrder.ascending
                )
            ]
        )
    ],
    facts=[
        Fact(
            table_name='factSales',
            display_name='Sales',
            editable=True,
            display_rows=10000,
            show_on_load=False,
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
            ],
            order_by=[
                OrderBy(
                    field_name='Customer',
                    sort_order=SortOrder.ascending
                )
            ]
        )
    ],
    views=[
        View(
            view_display_name='Customer Totals',
            fact_table_name='factSales',
            group_by_field_names=['Customer'],
            aggregate_field_names=['Sales Amount'],
            show_on_load=True
        )
    ]
)

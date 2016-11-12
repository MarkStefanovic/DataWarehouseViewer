from sqlalchemy import func

from star_schema.constellation import (
    AdditiveField,
    CalculatedField,
    Constellation,
    Dimension,
    Fact,
    Field,
    ForeignKey,
    SummaryField,
    View,
)
from star_schema.custom_types import (
    FieldFormat,
    FieldType,
    FilterConfig,
    Operator,
    OrderBy,
    SortOrder
)


class ConfigError(Exception):
    def __init__(self, message: str):
        self.message = message
        print(message)


class App:
    def __init__(self, *,
        color_scheme: str,
        db_path: str,
        display_name: str
    ) -> None:

        self.color_scheme = color_scheme
        self.db_path = db_path
        self.display_name = display_name
'''
cfg = Constellation(
    app=App(
        display_name='IReadGud',
        color_scheme='darkcity.css',
        db_path='sqlite:///ireadgud.db'
    ),
    dimensions=[
        Dimension(
            table_name='dimAuthors',
            display_name='Authors',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='FullName',
                    dtype=FieldType.Str,
                    display_name='Author',
                    filter_operators=[Operator.str_like]
                )
            ],
            summary_field=SummaryField(
                display_fields=['FullName'],
                display_name='Author',
                separator=' - ',
                filter_operators=[Operator.str_like]
            ),
            order_by=[
                OrderBy(
                    field_name='FullName',
                    sort_order=SortOrder.Ascending
                )
            ]
        )
        , Dimension(
            table_name='dimBookTypes',
            display_name='Book Types',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='BookType',
                    dtype=FieldType.Str,
                    display_name='Book Type',
                    filter_operators=[Operator.str_like]
                )
            ],
            summary_field=SummaryField(
                display_fields=['BookType'],
                display_name='Book Type',
                separator=' - ',
                filter_operators=[Operator.str_like]
            ),
            order_by=[
                OrderBy(
                    field_name='BookType',
                    sort_order=SortOrder.Ascending
                )
            ]
        ),
        Dimension(
            table_name='dimCompilations',
            display_name='Compilations',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='Compilation',
                    dtype=FieldType.Str,
                    display_name='Compilation',
                    filter_operators=[Operator.str_like]
                )
            ],
            summary_field=SummaryField(
                display_fields=['Compilation'],
                display_name='Compilation',
                separator=' - ',
                filter_operators=[Operator.str_like]
            ),
            order_by=[
                OrderBy(
                    field_name='Compilation',
                    sort_order=SortOrder.Ascending
                )
            ]
        ),
        Dimension(
            table_name='dimFormats',
            display_name='Formats',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='Format',
                    dtype=FieldType.Str,
                    display_name='Book Format',
                    filter_operators=[Operator.str_like]
                )
            ],
            summary_field=SummaryField(
                display_fields=['Format'],
                display_name='Book Format',
                separator=' - ',
                filter_operators=[Operator.str_like]
            ),
            order_by=[
                OrderBy(
                    field_name='Format',
                    sort_order=SortOrder.Ascending
                )
            ]
        ),
        Dimension(
            table_name='dimGenres',
            display_name='Genres',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='Genre',
                    dtype=FieldType.Str,
                    display_name='Genre',
                    filter_operators=[Operator.str_like]
                )
            ],
            summary_field=SummaryField(
                display_fields=['Genre'],
                display_name='Genre',
                separator=' - ',
                filter_operators=[Operator.str_like]
            ),
            order_by=[
                OrderBy(
                    field_name='Genre',
                    sort_order=SortOrder.Ascending
                )
            ]
        )
    ],
    facts=[
        Fact(
            table_name='factBooks',
            display_name='Books',
            editable=True,
            display_rows=1000,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='BookName',
                    dtype=FieldType.Str,
                    display_name='Title',
                    field_format=FieldFormat.Str,
                    filter_operators=[Operator.str_like]
                ),
                ForeignKey(
                    name='AuthorID',
                    display_name='Author',
                    dimension='dimAuthors',
                    foreign_key_field='ID'
                ),
                ForeignKey(
                    name='FormatID',
                    display_name='Book Format',
                    dimension='dimFormats',
                    foreign_key_field='ID'
                ),
                ForeignKey(
                    name='BookTypeID',
                    display_name='Book Type',
                    dimension='dimBookTypes',
                    foreign_key_field='ID'
                ),
                ForeignKey(
                    name='GenreID',
                    display_name='Genre',
                    dimension='dimGenres',
                    foreign_key_field='ID'
                ),
                ForeignKey(
                    name='CompilationID',
                    display_name='Compilation',
                    dimension='dimCompilations',
                    foreign_key_field='ID'
                ),
                Field(
                    name='YearPublished',
                    dtype=FieldType.Int,
                    display_name='Year Published',
                    field_format=FieldFormat.Int,
                    filter_operators=[
                        # Operator.number_greater_than_or_equal_to,
                        # Operator.number_less_than_or_equal_to
                    ]
                ),
                Field(
                    name='DateFinished',
                    dtype=FieldType.Date,
                    display_name='Date Finished',
                    field_format=FieldFormat.Date,
                    filter_operators=[
                        Operator.date_on_or_after,
                        Operator.date_on_or_before,
                    ]
                ),
                Field(
                    name='Finished',
                    dtype=FieldType.Bool,
                    display_name='Finished?',
                    field_format=FieldFormat.Bool,
                    filter_operators=[]
                ),
                Field(
                    name='WantScale',
                    dtype=FieldType.Int,
                    display_name='Want Scale',
                    field_format=FieldFormat.Int,
                    filter_operators=[]
                ),
                Field(
                    name='FavoriteScale',
                    dtype=FieldType.Int,
                    display_name='Favorite Scale',
                    field_format=FieldFormat.Int,
                    filter_operators=[]
                ),
                Field(
                    name='Owned',
                    dtype=FieldType.Bool,
                    display_name='Owned?',
                    field_format=FieldFormat.Bool,
                    filter_operators=[]
                ),
                Field(
                    name='BookNotes',
                    dtype=FieldType.Str,
                    display_name='Notes',
                    field_format=FieldFormat.Str,
                    filter_operators=[Operator.str_like]
                )
            ],
            order_by=[
                OrderBy(
                    field_name='Author',
                    sort_order=SortOrder.Ascending
                )
            ],
            calculated_fields=[]
        )
    ],
    views=[
        View(
            view_display_name='Books by Author',
            fact_table_name='factBooks',
            group_by_field_names=['Author'],
            additive_fields=[
                AdditiveField(
                    base_field_display_name='Title',
                    aggregate_display_name='Stories',
                    aggregate_func=func.count
                ),
                AdditiveField(
                    base_field_display_name='Favorite Scale',
                    aggregate_display_name='Avg Fav. Score',
                    aggregate_func=func.avg
                ),
                AdditiveField(
                    base_field_display_name='Owned?',
                    aggregate_display_name='Stories Owned',
                    aggregate_func=func.sum
                ),
                AdditiveField(
                    base_field_display_name='Finished?',
                    aggregate_display_name='Stories Read',
                    aggregate_func=func.sum
                ),
            ],
            order_by=[
                OrderBy(
                    field_name='Avg Fav. Score',
                    sort_order=SortOrder.Descending
                )
            ],
            show_on_load=True
        )
    ]
)
'''


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
                    dtype=FieldType.Int,
                    display_name='ID',
                    primary_key=True
                ),
                Field(
                    name='ProductName',
                    dtype=FieldType.Str,
                    display_name='Name',
                    filters=[
                        FilterConfig(operator=Operator.str_like,
                                     default_value='')
                    ]
                ),
                Field(
                    name='ProductCategory',
                    dtype=FieldType.Str,
                    display_name='Category',
                    filters=[
                        FilterConfig(operator=Operator.str_like,
                                     default_value='')
                    ]
                )
            ],
            summary_field=SummaryField(
                display_fields=['ProductName', 'ProductCategory'],
                display_name='Product',
                separator=' - ',
                filters=[
                    FilterConfig(operator=Operator.str_like,
                                 default_value=''),
                ]
            ),
            order_by=[
                OrderBy(
                    field_name='ProductName',
                    sort_order=SortOrder.Ascending
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
                    dtype=FieldType.Int,
                    display_name='Customer ID',
                    primary_key=True
                ),
                Field(
                    name='CustomerName',
                    dtype=FieldType.Str,
                    display_name='Customer Name',
                    filters=[
                        FilterConfig(operator=Operator.str_like,
                                     default_value='Mark')
                    ]
                ),
                Field(
                    name='ShippingAddress',
                    dtype=FieldType.Str,
                    display_name='Shipping Address',
                    filters=[
                        FilterConfig(operator=Operator.str_like,
                                     default_value='')
                    ]
                ),
            ],
            summary_field=SummaryField(
                display_fields=['CustomerName'],
                display_name='Customer',
                separator=' - ',
                filters=[
                    FilterConfig(operator=Operator.str_like,
                                 default_value='')
                ]
            ),
            order_by=[
                OrderBy(
                    field_name='CustomerName',
                    sort_order=SortOrder.Ascending
                )
            ]
        )
    ],
    facts=[
        Fact(
            table_name='factSales',
            display_name='Sales',
            editable=True,
            display_rows=1000,
            show_on_load=True,
            fields=[
                Field(
                    name='OrderID',
                    dtype=FieldType.Int,
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
                    dtype=FieldType.Date,
                    display_name='Order Date',
                    filters=[
                        FilterConfig(operator=Operator.date_on_or_before,
                                     default_value=''),
                        FilterConfig(operator=Operator.date_on_or_after,
                                     default_value='2015-01-01'),
                    ]
                ),
                Field(
                    name='ShippingDate',
                    dtype=FieldType.Date,
                    display_name='Shipping Date',
                    filters=[
                        FilterConfig(operator=Operator.date_on_or_before,
                                     default_value=''),
                        FilterConfig(operator=Operator.date_on_or_after,
                                     default_value='2015-01-01'),
                    ]
                ),
                Field(
                    name='SalesAmount',
                    dtype=FieldType.Float,
                    display_name='Sales Amount',
                    field_format=FieldFormat.Accounting,
                    filters=[
                        FilterConfig(operator=Operator.number_greater_than_or_equal_to,
                                     default_value='0.00')
                    ]
                ),
                Field(
                    name='Paid',
                    dtype=FieldType.Bool,
                    display_name='Paid',
                    field_format=FieldFormat.Str,
                    filters=[]
                ),
            ],
            calculated_fields=[
                CalculatedField(
                    formula='[Sales Amount] - ([Paid] * [Sales Amount])',
                    display_name='Amount Due',
                    show_on_fact_table=True
                )
            ],
            order_by=[
                OrderBy(
                    field_name='Customer',
                    sort_order=SortOrder.Ascending
                )
            ]
        )
    ],
    views=[
        View(
            view_display_name='Customer Totals',
            fact_table_name='factSales',
            group_by_field_names=['Customer'],
            additive_fields=[
                AdditiveField(
                    base_field_display_name='Sales Amount',
                    aggregate_display_name='Transactions',
                    aggregate_func=func.count
                ),
                AdditiveField(
                    base_field_display_name='Sales Amount',
                    aggregate_display_name='Total Sales',
                    aggregate_func=func.sum
                ),
                AdditiveField(
                    base_field_display_name='Sales Amount',
                    aggregate_display_name='Average Sale',
                    aggregate_func=func.avg
                ),
                AdditiveField(
                    base_field_display_name='Amount Due',
                    aggregate_display_name='Total Amount Due',
                    aggregate_func=func.sum
                )
            ],
            show_on_load=True
        ),
        View(
            view_display_name='Product Totals',
            fact_table_name='factSales',
            group_by_field_names=['Product'],
            additive_fields=[
                AdditiveField(
                    base_field_display_name='Sales Amount',
                    aggregate_display_name='Transactions',
                    aggregate_func=func.count
                ),
                AdditiveField(
                    base_field_display_name='Sales Amount',
                    aggregate_display_name='Total Sales',
                    aggregate_func=func.sum
                ),
                AdditiveField(
                    base_field_display_name='Sales Amount',
                    aggregate_display_name='Average Sale',
                    aggregate_func=func.avg
                )
            ],
            show_on_load=True
        )
    ]
)

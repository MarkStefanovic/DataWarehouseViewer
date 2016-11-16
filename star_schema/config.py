import datetime

from sqlalchemy import func

from logger import rotating_log
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


class DefaultDates:
    now = datetime.datetime.now().isoformat()
    today = datetime.datetime.today().strftime("%Y-%m-%d")


class ConfigError(Exception):
    def __init__(self, message: str):
        self.message = message
        rotating_log('config.ConfigError').error(message)


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
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='ProductCategory',
                    dtype=FieldType.Str,
                    display_name='Category',
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                )
            ],
            summary_field=SummaryField(
                display_fields=['ProductName', 'ProductCategory'],
                display_name='Product',
                separator=' - ',
                filters=[
                    FilterConfig(
                        operator=Operator.str_like,
                        default_value=''
                    ),
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
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value='Mark'
                        )
                    ]
                ),
                Field(
                    name='ShippingAddress',
                    dtype=FieldType.Str,
                    display_name='Shipping Address',
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
            ],
            summary_field=SummaryField(
                display_fields=['CustomerName'],
                display_name='Customer',
                separator=' - ',
                filters=[
                    FilterConfig(
                        operator=Operator.str_like,
                        default_value=''
                    )
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
                        FilterConfig(
                            operator=Operator.date_on_or_before,
                            default_value=''
                        ),
                        FilterConfig(
                            operator=Operator.date_on_or_after,
                            default_value='2015-01-01'
                        ),
                    ]
                ),
                Field(
                    name='ShippingDate',
                    dtype=FieldType.Date,
                    display_name='Shipping Date',
                    filters=[
                        FilterConfig(
                            operator=Operator.date_on_or_before,
                            default_value=''
                        ),
                        FilterConfig(
                            operator=Operator.date_on_or_after,
                            default_value='2015-01-01'
                        ),
                    ]
                ),
                Field(
                    name='SalesAmount',
                    dtype=FieldType.Float,
                    display_name='Sales Amount',
                    field_format=FieldFormat.Accounting,
                    filters=[
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value='0.00'
                        )
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
                    formula='[Sales Amount] * [1.04]',
                    display_name='Sales with Tax',
                    show_on_fact_table=True,
                    filters=[
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value='10.00'
                        )
                    ]
                ),
                CalculatedField(
                    formula='[1.04] * ([Sales Amount] - ([Paid] * [Sales Amount]))',
                    display_name='Amount Due',
                    show_on_fact_table=True,
                    filters=[
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value='0.00'
                        )
                    ]
                )
            ],
            order_by=[
                OrderBy(
                    field_name='Amount Due',
                    sort_order=SortOrder.Descending
                )
            ],
            refresh_on_update=True
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
                    base_field_display_name='Sales with Tax',
                    aggregate_display_name='Total Sales',
                    aggregate_func=func.sum
                ),
                AdditiveField(
                    base_field_display_name='Sales with Tax',
                    aggregate_display_name='Average Sale',
                    aggregate_func=func.avg
                ),
                AdditiveField(
                    base_field_display_name='Amount Due',
                    aggregate_display_name='Total Amount Due',
                    aggregate_func=func.sum
                )
            ],
            order_by=[
                OrderBy(
                    field_name='Total Amount Due',
                    sort_order=SortOrder.Descending
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
'''


cfg = Constellation(
    app=App(
        display_name='Submission Tracker',
        color_scheme='darkcity.css',
        db_path='sqlite:///submission_tracker.db'
    ),
    dimensions=[
        Dimension(
            table_name='dimPublication',
            display_name='Publications',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='Publication ID',
                    primary_key=True
                ),
                Field(
                    name='PublicationName',
                    dtype=FieldType.Str,
                    display_name='Name',
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='PublicationGenres',
                    dtype=FieldType.Str,
                    display_name='Genres',
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='SimultaneousSubmissions',
                    dtype=FieldType.Bool,
                    display_name='Simultaneous Submissions',
                    filters=[]
                ),
                Field(
                    name='Notes',
                    dtype=FieldType.Str,
                    display_name='Publication Notes',
                    field_format=FieldFormat.Str,
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
            ],
            summary_field=SummaryField(
                display_fields=['PublicationName', 'PublicationGenres'],
                display_name='Publication',
                separator=' - ',
                filters=[
                    FilterConfig(
                        operator=Operator.str_like,
                        default_value=''
                    ),
                ]
            ),
            order_by=[
                OrderBy(
                    field_name='PublicationName',
                    sort_order=SortOrder.Ascending
                )
            ]
        )
        , Dimension(
            table_name='dimStory',
            display_name='Stories',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='Story ID',
                    primary_key=True
                ),
                Field(
                    name='Title',
                    dtype=FieldType.Str,
                    display_name='Title',
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='Genres',
                    dtype=FieldType.Str,
                    display_name='Genre(s)',
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='DateFinished',
                    dtype=FieldType.Date,
                    display_name='Date Finished',
                    default_value=DefaultDates.today,
                    filters=[
                        FilterConfig(
                            operator=Operator.date_on_or_after,
                            default_value=''
                        ),
                        FilterConfig(
                            operator=Operator.date_on_or_before,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='WordLength',
                    dtype=FieldType.Int,
                    display_name='Word Length',
                    default_value=0,
                    filters=[
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value=''
                        ),
                        FilterConfig(
                            operator=Operator.number_less_than_or_equal_to,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='Notes',
                    dtype=FieldType.Str,
                    display_name='Story Notes',
                    field_format=FieldFormat.Str,
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
            ],
            summary_field=SummaryField(
                display_fields=['Title', 'Genres'],
                display_name='Story',
                separator=' - ',
                filters=[
                    FilterConfig(
                        operator=Operator.str_like,
                        default_value=''
                    )
                ]
            ),
            order_by=[
                OrderBy(
                    field_name='Title',
                    sort_order=SortOrder.Ascending
                )
            ]
        )
        , Dimension(
            table_name='dimStatus',
            display_name='Statuses',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='Status ID',
                    primary_key=True
                ),
                Field(
                    name='Status',
                    dtype=FieldType.Str,
                    display_name='Status',
                    filters=[
                        # FilterConfig(
                        #     operator=Operator.str_like,
                        #     default_value=''
                        # )
                    ]
                )
            ],
            summary_field=SummaryField(
                display_fields=['Status'],
                display_name='Status',
                separator=' - ',
                filters=[
                    FilterConfig(
                        operator=Operator.str_like,
                        default_value=''
                    )
                ]
            ),
            order_by=[
                OrderBy(
                    field_name='Status',
                    sort_order=SortOrder.Ascending
                )
            ]
        )
    ],
    facts=[
        Fact(
            table_name='factSubmission',
            display_name='Submissions',
            editable=True,
            display_rows=1000,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='Status ID',
                    primary_key=True
                ),
                ForeignKey(
                    name='StoryID',
                    display_name='Story',
                    dimension='dimStory',
                    foreign_key_field='ID'
                ),
                ForeignKey(
                    name='PublicationID',
                    display_name='Publication',
                    dimension='dimPublication',
                    foreign_key_field='ID'
                ),
                ForeignKey(
                    name='StatusID',
                    display_name='Status',
                    dimension='dimStatus',
                    foreign_key_field='ID'
                ),
                Field(
                    name='DateSubmitted',
                    dtype=FieldType.Date,
                    display_name='Date Submitted',
                    default_value=DefaultDates.today,
                    filters=[
                        FilterConfig(
                            operator=Operator.date_on_or_before,
                            default_value=''
                        ),
                        FilterConfig(
                            operator=Operator.date_on_or_after,
                            default_value=''
                        ),
                    ]
                ),
                Field(
                    name='Revenue',
                    dtype=FieldType.Float,
                    display_name='Revenue',
                    field_format=FieldFormat.Accounting,
                    filters=[
                        # FilterConfig(
                        #     operator=Operator.number_greater_than_or_equal_to,
                        #     default_value='0.00'
                        # )
                    ]
                ),
                Field(
                    name='Expenses',
                    dtype=FieldType.Float,
                    display_name='Expenses',
                    field_format=FieldFormat.Accounting,
                    filters=[
                        # FilterConfig(
                        #     operator=Operator.number_greater_than_or_equal_to,
                        #     default_value='0.00'
                        # )
                    ]
                ),
                Field(
                    name='Response',
                    dtype=FieldType.Str,
                    display_name='Response(s)',
                    field_format=FieldFormat.Str,
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='Notes',
                    dtype=FieldType.Str,
                    display_name='Submission Notes',
                    field_format=FieldFormat.Str,
                    filters=[
                        FilterConfig(
                            operator=Operator.str_like,
                            default_value=''
                        )
                    ]
                ),
            ],
            calculated_fields=[
                CalculatedField(
                    formula='[Revenue] - [Expenses]',
                    display_name='Income',
                    show_on_fact_table=True,
                    filters=[
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value=''
                        )
                    ]
                )
            ],
            order_by=[
                OrderBy(
                    field_name='Date Submitted',
                    sort_order=SortOrder.Descending
                )
            ],
            refresh_on_update=False
        )
    ],
    views=[
        View(
            view_display_name='Publication Totals',
            fact_table_name='factSubmission',
            group_by_field_names=['Publication', 'Status'],
            additive_fields=[
                AdditiveField(
                    base_field_display_name='Date Submitted',
                    aggregate_display_name='Stories Submitted',
                    aggregate_func=func.count
                ),
                AdditiveField(
                    base_field_display_name='Income',
                    aggregate_display_name='Total Income',
                    aggregate_func=func.sum
                ),
            ],
            order_by=[
                OrderBy(
                    field_name='Publication',
                    sort_order=SortOrder.Ascending
                )
            ],
            show_on_load=True
        )
    ]
)

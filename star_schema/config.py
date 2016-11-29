import datetime
import json
import os
from enum import Enum
from functools import lru_cache

import logging
from sqlalchemy import func
from typing import Dict

from star_schema.custom_types import (
    FieldFormat,
    FieldType,
    Operator,
    OrderBy,
    SortOrder,
)
from star_schema.utilities import autorepr

module_logger = logging.getLogger('app.' + __name__)


defaults = {
    'Now': lambda: datetime.datetime.now().isoformat(),
    'Today': lambda: datetime.datetime.today().strftime("%Y-%m-%d"),
    'Username': lambda: os.getlogin()
}


class AppConfig:
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('AppConfig')
        self.config = config
        try:
            self.color_scheme = config['color_scheme']
            self.db_path = config['db_path']
            self.display_name = config['display_name']
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for App;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for App;'
                'error {}'.format(str(e))
            )
            raise


@autorepr
class FieldConfig:
    def __init__(self, config) -> None:
        self.logger = module_logger.getChild('FieldConfig')
        self._config = config  # type: Dict
        try:
            self.name = config['name']  # type: str
            self.dtype = FieldType[config['dtype']]  # type: FieldType
            self.display_name = config['display_name']  # type: str
            self.primary_key = config.get('primary_key', False)  # type: bool
            self.field_format = FieldFormat[config.get('field_format', 'Str')]
            self.editable = config.get('editable', True)  # type: bool
            default_val = defaults.get(config.get('default_value'))
            if default_val:
                self.default_value = default_val()
            else:
                self.default_value = config.get('default_value')
            self.visible = config.get('visible', True)
            self.validator = None  # TODO
            self.filters = [
                FilterConfig(flt_cfg, self)
                for flt_cfg in config.get('filters', [])
            ]
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for field;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for field;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class FilterConfig:
    """Configuration for a filter"""
    def __init__(self, config: Dict, field: FieldConfig) -> None:
        self.logger = module_logger.getChild('FilterConfig')
        self._config = config
        try:
            self.default_value = config.get('default_value')
            self.operator = Operator[config['operator']]
            self.field = field
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for filter;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for filter;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class CalculatedFieldConfig:
    def __init__(self, config) -> None:
        self.logger = module_logger.getChild('CalculatedFieldConfig')
        self._config = config  # type: Dict
        try:
            self.name = config['display_name']
            self.dtype = FieldType[config.get('dtype', 'Float')]
            self.field_format = FieldFormat[config.get('field_format', 'Accounting')]
            self.formula = config['formula']  # type: str
            self.display_name = config['display_name']  # type: str
            self.show_on_fact_table = config.get('show_on_fact_table', False)  # type: bool
            self.filters = [
                FilterConfig(flt_cfg, self)
                for flt_cfg in config.get('filters', [])
            ]
            self.default_value = 0
            self.editable = False
            self.primary_key = False
            self.validator = None  # TODO
            self.visible = config.get('visible', True)
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for calculated field;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for calculated field;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class SummaryFieldConfig:
    """
    This class needs to provide the same attributes as a regular Field.
    """
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('SummaryField')
        self._config = config  # type: Dict
        try:
            self.name = None
            self.dtype = FieldType.Str
            self.field_format = FieldFormat.Str
            self.display_fields = config['display_fields']  # type: List[str]
            self.display_name = config['display_name']  # type: str
            self.separator = config.get('separator', ' - ')  # type: str
            self.filters = [
                FilterConfig(flt_cfg, self)
                for flt_cfg in config.get('filters', [])
            ]  # type: List[FilterConfig]
            self.editable = False
            self.primary_key = False
            self.default_value = ''
            self.visible = False
            self.validator = None
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for summary field;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for summary field;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class ForeignKeyConfig:
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('ForeignKeyConfig')
        self._config = config
        try:
            self.name = config['name']
            self.display_name = config['display_name']
            self.dtype = FieldType.Int
            self.field_format = FieldFormat.Str
            self.dimension = config['dimension']
            self.foreign_key_field = config['foreign_key_field']
            self.editable = config.get('editable', True)
            self.filters = [
                FilterConfig(flt_cfg)
                for flt_cfg in config.get('filters', [])
            ]
            self.default_value = config.get('default_value', 0)
            self.visible = config.get('visible', True)
            self.validator = None # TODO
            self.primary_key = False
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for ForeignKey;'
                'missing a required field; error {}'.format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for ForeignKey;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class DimensionConfig:
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('DimensionConfig')
        self._config = config  # type: Dict
        try:
            self.table_name = config['table_name']  # type: str
            self.display_name = config['display_name']  # type: str
            self.editable = config.get('editable', False)  # type: bool
            self.show_on_load = config.get('show_on_load', True)  # type: bool
            self.summary_field = SummaryFieldConfig(config['summary_field'])
            self.display_rows = config.get('display_rows', 10000)
            self.refresh_on_update = config.get('refresh_on_update', False)
            self.order_by = [
                OrderBy(field_name=ord_cfg['field_name'],
                        sort_order=SortOrder[ord_cfg['sort_order']])
                for ord_cfg in config.get('order_by', [])
            ]
            self.fields = [
                FieldConfig(fld_cfg)
                for fld_cfg in config['fields']
            ]
            self.foreign_keys = [
                ForeignKeyConfig(fk_cfg)
                for fk_cfg in config.get('foreign_keys', [])
            ]
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for Dimension;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for Dimension;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class LookupTableConfig:
    """
    This class needs to have the same attributes as DimensionConfig.
    """
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('LookupTableConfig')
        self._config = config
        try:
            self.table_name = config['table_name']  # type: str
            self.display_name = config['display_name']  # type: str
            self.id_field = FieldConfig(config['id_field'])
            self.proximal_fk = ForeignKeyConfig(config['proximal_fk'])
            self.distal_fk = ForeignKeyConfig(config['distal_fk'])
            self.editable = config.get('editable', True)  # type: bool
            self.show_on_load = config['show_on_load']  # type: bool
            self.summary_field = SummaryFieldConfig(config['summary_field'])
            self.display_rows = config.get('display_rows', 10000)
            self.fields = [
                self.id_field,
                self.proximal_fk,
                self.distal_fk
            ]
            self.order_by = [
                OrderBy(field_name=ord_cfg['field_name'],
                        sort_order=SortOrder[ord_cfg['sort_order']])
                for ord_cfg in config.get('order_by', [])
            ]
            self.extra_fields = [
                FieldConfig(fld_cfg)
                for fld_cfg in config.get('extra_fields', [])
            ]
            self.refresh_on_update = False
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for LookupTable;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for LookupTable;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class FactConfig:
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('FactConfig')
        self._config = config  # type: Dict
        try:
            self.table_name = config['table_name']  # type: str
            self.display_name = config['display_name']  # type: str
            self.editable = config.get('editable', False)  # type: bool
            self.show_on_load = config.get('show_on_load', True)  # type: bool
            self.display_rows = config.get('display_rows', 10000)  # type: int
            self.refresh_on_update = config.get('refresh_on_update', False)
            self.order_by = [
                OrderBy(field_name=ord_cfg['field_name'],
                        sort_order=SortOrder[ord_cfg['sort_order']])
                for ord_cfg in config.get('order_by', [])
            ]
            self.fields = [
                FieldConfig(fld_cfg)
                for fld_cfg in config['fields']
            ]
            self.foreign_keys = [
                ForeignKeyConfig(fk_cfg)
                for fk_cfg in config.get('foreign_keys', [])
            ]
            self.calculated_fields = [
                CalculatedFieldConfig(calc_cfg)
                for calc_cfg in config.get('calculated_fields', [])
            ]
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for Fact;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for Fact;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class AdditiveFieldConfig:
    """
    :type config: Dict[str, Union[List, str, bool]]
    :param config:
        base_field_display_name:
            the display name of the Star field to aggregate.
            This can be a Field, SummaryField, or a CalculatedField.
        aggregate_display_name:
            text to use for the output field's header
        aggregate_func:
            instance of SqlAlchemy func enum to aggregate the field by
        visible
            Display the field on the views?
    """
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('AdditiveFieldConfig')
        self._config = config  # type: Dict
        try:
            self.base_field_display_name = config['base_field_display_name']  # type: str
            self.aggregate_display_name = config['aggregate_display_name']  # type: str
            self.aggregate_func = getattr(func, config['aggregate_func'])  # type: bool
            self.order_by = [
                OrderBy(field_name=ord_cfg['field_name'],
                        sort_order=SortOrder[ord_cfg['sort_order']])
                for ord_cfg in config.get('order_by', [])
            ]
            self.visible = config.get('visible', True)
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for AdditiveField;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for AdditiveField;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class ViewConfig:
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('ViewConfig')
        self._config = config  # type: Dict
        try:
            self.view_display_name = config['view_display_name']  # type: str
            self.fact_table_name = config['fact_table_name']  # type: str
            self.show_on_load = config.get('show_on_load', True)  # type: bool
            self.refresh_on_update = config.get('refresh_on_update', False)
            self.group_by_field_names = config.get('group_by_field_names', [])
            self.order_by = [
                OrderBy(field_name=ord_cfg['field_name'],
                        sort_order=SortOrder[ord_cfg['sort_order']])
                for ord_cfg in config.get('order_by', [])
            ]
            self.additive_fields = [
                AdditiveFieldConfig(add_cfg)
                for add_cfg in config.get('additive_fields', [])
            ]
            self.validate()
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for View;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for View;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@autorepr
class ConstellationConfig:
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('ConstellationConfig')
        self._config = config  # type: Dict
        try:
            self.app = AppConfig(config['app'])
            self.dimensions = [
                DimensionConfig(dim_cfg)
                for dim_cfg in config.get('dimensions', [])
            ]
            self.lookup_tables = [
                LookupTableConfig(lkp_cfg)
                for lkp_cfg in config.get('lookup_tables', [])
            ]
            self.facts = [
                FactConfig(fact_cfg)
                for fact_cfg in config['facts']
            ]
            self.views = [
                ViewConfig(view_cfg)
                for view_cfg in config['views']
            ]
        except (KeyError, AttributeError) as e:
            self.logger.error(
                'Invalid configuration for Constellation;'
                'missing a required field; error {}'
                .format(str(e))
            )
            raise
        except Exception as e:
            self.logger.error(
                'Invalid configuration for Constellation;'
                'error {}'.format(str(e))
            )
            raise

    def validate(self):
        pass


@lru_cache(maxsize=1)
def get_config(json_path: str=os.path.join('constellations', 'default.json')) -> Dict:
    with open(json_path) as fh:
        return json.load(fh)




'''
cfg = Constellation(
    app=App(
        display_name='Submission Tracker',
        color_scheme='darkcity.css',
        db_path='sqlite:///submission_tracker.db'
    ),
    dimensions=[
        Dimension(
            table_name='dimGenre',
            display_name='Genres',
            editable=True,
            show_on_load=True,
            fields=[
                Field(
                    name='ID',
                    dtype=FieldType.Int,
                    display_name='Genre ID',
                    primary_key=True
                ),
                Field(
                    name='Genre',
                    dtype=FieldType.Str,
                    display_name='Genre',
                    filters=[]
                )
            ],
            summary_field=SummaryField(
                display_fields=['Genre'],
                display_name='Genre',
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
                    field_name='Genre',
                    sort_order=SortOrder.Ascending
                )
            ]
        ),
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
                # ManyToManyField(
                #     lookup_table_name='lkpPublicationGenre',
                #     display_name='Genre(s)',
                #     field_separator='; '
                # ),
                Field(
                    name='SimultaneousSubmissions',
                    dtype=FieldType.Bool,
                    display_name='Simultaneous Submissions',
                    default_value=False,
                    filters=[
                        FilterConfig(
                            operator=Operator.bool_is,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='PreferredScore',
                    dtype=FieldType.Int,
                    display_name='Pref.Score: 1-5',
                    default_value=1,
                    filters=[
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value=1
                        )
                    ]
                ),
                Field(
                    name='MinimumWordCount',
                    dtype=FieldType.Int,
                    display_name='Min Word Ct',
                    default_value=50,
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
                    name='MaximumWordCount',
                    dtype=FieldType.Int,
                    display_name='Max Word Ct',
                    default_value=10000,
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
                    name='UnderConsideration',
                    dtype=FieldType.Bool,
                    display_name='Under Consideration',
                    filters=[
                        FilterConfig(
                            operator=Operator.bool_is,
                            default_value=''
                        )
                    ]
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
                Field(
                    name='DateAdded',
                    dtype=FieldType.Str,
                    editable=False,
                    display_name='Date Added',
                    default_value=Defaults.Today,
                    field_format=FieldFormat.Date,
                    filters=[],
                    visible=True
                )
            ],
            summary_field=SummaryField(
                display_fields=['PublicationName'],
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
                # ManyToManyField(
                #     lookup_table_name='lkpStoryGenre',
                #     display_name='Genre(s)',
                #     field_separator='; '
                # ),
                Field(
                    name='DateFinished',
                    dtype=FieldType.Date,
                    display_name='Date Finished',
                    default_value=Defaults.Today,
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
                Field(
                    name='DateAdded',
                    dtype=FieldType.Str,
                    editable=False,
                    display_name='Date Added',
                    default_value=Defaults.Today,
                    field_format=FieldFormat.Date,
                    filters=[],
                    visible=True
                )
            ],
            summary_field=SummaryField(
                display_fields=['Title'],
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
                    filters=[]
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
                    default_value=Defaults.Today,
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
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value=''
                        )
                    ]
                ),
                Field(
                    name='Expenses',
                    dtype=FieldType.Float,
                    display_name='Expenses',
                    field_format=FieldFormat.Accounting,
                    filters=[
                        FilterConfig(
                            operator=Operator.number_greater_than_or_equal_to,
                            default_value=''
                        )
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
                Field(
                    name='DateAdded',
                    dtype=FieldType.Str,
                    editable=False,
                    display_name='Date Added',
                    default_value=Defaults.Today,
                    field_format=FieldFormat.Date,
                    filters=[],
                    visible=True
                )
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
    # nexus dimension of a many-to-many relationships
    lookup_tables= [
        LookupTable(
            table_name='lkpStoryGenre',
            display_name='Story Genre Lookup',
            editable=True,
            show_on_load=True,
            id_field=Field(
                name='ID',
                dtype=FieldType.Int,
                display_name='ID',
                primary_key=True
            ),
            proximal_fk=ForeignKey(
                name='StoryID',
                display_name='Story',
                dimension='dimStory',
                foreign_key_field='ID'
            ),
            distal_fk=ForeignKey(
                name='GenreID',
                display_name='Genre(s)',
                dimension='dimGenre',
                foreign_key_field='ID'
            ),
            extra_fields=[],
            summary_field=SummaryField(
                display_fields=['GenreID'],
                display_name='Genre(s)',
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
                    field_name='GenreID',
                    sort_order=SortOrder.Ascending
                )
            ]
        ),
        LookupTable(
            table_name='lkpPublicationGenre',
            display_name='Publication Genre Lookup',
            editable=True,
            show_on_load=True,
            id_field=Field(
                name='ID',
                dtype=FieldType.Int,
                display_name='ID',
                primary_key=True
            ),
            proximal_fk=ForeignKey(
                name='PublicationID',
                display_name='Publication',
                dimension='dimPublication',
                foreign_key_field='ID'
            ),
            distal_fk=ForeignKey(
                name='GenreID',
                display_name='Genre(s)',
                dimension='dimGenre',
                foreign_key_field='ID'
            ),
            extra_fields=[],
            summary_field=SummaryField(
                display_fields=['GenreID'],
                display_name='Genre(s)',
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
                    field_name='GenreID',
                    sort_order=SortOrder.Ascending
                )
            ]
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
'''

default_config = ConstellationConfig(get_config())

if __name__ == '__main__':
    print('default_config:', default_config)

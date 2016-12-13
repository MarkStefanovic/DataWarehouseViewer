import datetime
import json
import logging
import os
from functools import lru_cache

from sqlalchemy import func
from typing import Dict

from star_schema.custom_types import (
    FieldFormat,
    FieldType,
    Operator,
    OrderBy,
    SortOrder,
    TableName
)
from star_schema.utilities import autorepr
from star_schema.validators import (
    NonBlank)

module_logger = logging.getLogger('app.' + __name__)


class ConfigurationError(Exception):
    """Error indicating json file does not conform to a valid Constellation"""


defaults = {
    'Now': lambda: datetime.datetime.now().isoformat(),
    'Today': lambda: datetime.datetime.today().strftime("%Y-%m-%d"),
    'Username': lambda: os.getlogin()
}


@autorepr(str_attrs=['display_name'], repr_attrs=['_config'])
class AppConfig:
    db_path = NonBlank()
    display_name = NonBlank()

    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('AppConfig')
        self.config = config

        try:
            self.color_scheme = config.get('color_scheme', 'darkcity.css')
            self.db_path = config['db_path']
            self.display_name = config['display_name']
        except (KeyError, AttributeError) as e:
            err_msg = (
                'Invalid configuration for App; '
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for App; '
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)


@autorepr(str_attrs=['display_name'], repr_attrs=['_config', 'table_name'])
class FieldConfig:
    """Configuration for a database table field.
    :param config: (Dict)
      keys:
        name:            Field name on the database table
        dtype:           FieldType enum value representing data type
        display_name:    Name to display on the field's header
        field_format:    FieldFormat enum value indicating display format (e.g., decimal places)
        filters:         List of FilterConfig's representing list of filters to show on query designer
        editable:        Is this field editable?
        primary_key:     Is this field the primary key of the table?
        default_value:   When creating a new instance of the field, start with a default value for the field.
        visible          Display the field on the views?
        validator        Function to run when upddating adding rows
                            If the function returns False for the value
                            of the field then the transaction will be rejected.
    """
    table_name = NonBlank()
    name = NonBlank()
    display_name = NonBlank() #Unique('field_display_name')

    def __init__(self, config, table_name: TableName) -> None:
        self.logger = module_logger.getChild('FieldConfig')
        self._config = config  # type: Dict
        try:
            self.table_name = table_name
            self.name = config['name']  # type: str
            self.dtype = FieldType[config['dtype']]  # type: FieldType
            self.display_name = config['display_name']  # type: str
            self.primary_key = config.get('primary_key', False)  # type: bool
            self.field_format = FieldFormat[config.get('field_format', config['dtype'])]
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
            err_msg = (
                'Invalid configuration for field; '
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for field; '
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['field', 'operator', 'default_value'],
          repr_attrs=['_config', 'field'])
class FilterConfig:
    """Configuration for a filter

    :param field:           base Field the filter is based on
    :param operator:        Operator enum value to apply to field
    :param default_value:   value to use in filter on initial load of app
    """
    def __init__(self, config: Dict, field: FieldConfig) -> None:
        self.logger = module_logger.getChild('FilterConfig')
        self._config = config
        try:
            self.default_value = config.get('default_value')
            self.operator = Operator[config['operator']]
            self.field = field
        except (KeyError, AttributeError) as e:
            err_msg = (
                'Invalid configuration for filter;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for filter;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['display_name', 'formula'],
          repr_attrs=['_config', 'table_name'])
class CalculatedFieldConfig:
    """A field that represents the combination of one or more fields in a Star.

    :param formula:             String formula using field display names
    :param display_name:        Name of field to display on header
    :param show_on_fact_table:  Show this field on the main fact table view
                                Fields that are merely used as intermediates may
                                be not be useful to show.
    :param filters:             Filters to display on QueryDesigner for this field
    :param default_value:       Default value to display when a new record is added
    :param visible              Display the field on the views?
    """
    name = NonBlank()
    formula = NonBlank()
    display_name = NonBlank()

    def __init__(self, config, table_name: TableName) -> None:
        self.logger = module_logger.getChild('CalculatedFieldConfig')
        self._config = config  # type: Dict
        try:
            self.name = config['display_name']
            self.table_name = table_name
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
            err_msg = (
                'Invalid configuration for calculated field;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for calculated field;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        """Validate that the configuration settings are internally consistent"""


@autorepr(str_attrs=['display_name'], repr_attrs=['_config', 'table_name'])
class SummaryFieldConfig:
    """
    This class needs to provide the same attributes as a regular Field.
    """
    table_name = NonBlank()
    display_name = NonBlank()

    def __init__(self, config: Dict, table_name: TableName) -> None:
        self.logger = module_logger.getChild('SummaryField')
        self._config = config  # type: Dict
        try:
            self.table_name = table_name
            self.dtype = FieldType.Str
            self.field_format = FieldFormat.Str
            self.display_fields = config['display_fields']  # type: List[str]
            self.display_name = config['display_name']  # type: str
            self.name = self.display_name
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
            err_msg = (
                'Invalid configuration for summary field;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for summary field;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['display_name'], repr_attrs=['_config', 'table_name'])
class PrimaryKeyConfig(FieldConfig):
    def __init__(self, config: Dict, table_name: TableName) -> None:
        config['dtype'] = "Int"
        config['field_format'] = "Int"
        config['editable'] = False
        config['visible'] = False
        config['validator'] = None
        config['primary_key'] = True
        config['default_value'] = None
        super().__init__(config, table_name)


@autorepr(str_attrs=['display_name'], repr_attrs=['_config', 'table_name'])
class ForeignKeyConfig:
    def __init__(self, config: Dict, table_name: TableName) -> None:
        self.logger = module_logger.getChild('ForeignKeyConfig')
        self._config = config
        try:
            self.name = config['name']
            self.table_name = table_name
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
            err_msg = (
                'Invalid configuration for ForeignKey;'
                'missing a required field; error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for ForeignKey;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['display_name'], repr_attrs=['_config', 'db_path'])
class DimensionConfig:
    def __init__(self, config: Dict, db_path: str) -> None:
        self.logger = module_logger.getChild('DimensionConfig')
        self._config = config  # type: Dict
        try:
            self.db_path = config.get('db_path', db_path)
            self.table_name = config['table_name']  # type: str
            self.display_name = config['display_name']  # type: str
            self.primary_key = PrimaryKeyConfig(config['primary_key'], self.table_name)
            self.editable = config.get('editable', False)  # type: bool
            self.show_on_load = config.get('show_on_load', True)  # type: bool
            self.summary_field = SummaryFieldConfig(config['summary_field'],
                self.table_name)
            self.display_rows = config.get('display_rows', 10000)
            self.refresh_on_update = config.get('refresh_on_update', False)
            self.order_by = [
                OrderBy(field_name=ord_cfg['field_name'],
                        sort_order=SortOrder[ord_cfg['sort_order']])
                for ord_cfg in config.get('order_by', [])
            ]
            self.fields = [self.primary_key]
            self.fields += [
                FieldConfig(fld_cfg, self.table_name)
                for fld_cfg in config['fields']
            ]  # type: List[FieldConfig]
            self.foreign_keys = [
                ForeignKeyConfig(fk_cfg, self.table_name)
                for fk_cfg in config.get('foreign_keys', [])
            ]
            self.validate()
        except (KeyError, AttributeError) as e:
            err_msg = (
                'Invalid configuration for Dimension;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for Dimension;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['display_name'], repr_attrs=['_config', 'db_path'])
class LookupTableConfig:
    """
    This class needs to have the same attributes as DimensionConfig.
    """
    def __init__(self, config: Dict, db_path: str) -> None:
        self.logger = module_logger.getChild('LookupTableConfig')
        self._config = config
        try:
            self.db_path = config.get('db_path', db_path)
            self.table_name = config['table_name']  # type: str
            self.display_name = config['display_name']  # type: str
            self.id_field = PrimaryKeyConfig(config['id_field'], self.table_name)
            self.proximal_fk = ForeignKeyConfig(config['proximal_fk'], self.table_name)
            self.distal_fk = ForeignKeyConfig(config['distal_fk'], self.table_name)
            self.editable = config.get('editable', True)  # type: bool
            self.show_on_load = config['show_on_load']  # type: bool
            self.summary_field = SummaryFieldConfig(
                config['summary_field'], self.table_name)
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
                FieldConfig(fld_cfg, self.table_name)
                for fld_cfg in config.get('extra_fields', [])
            ]
            self.refresh_on_update = False
            self.validate()
        except (KeyError, AttributeError) as e:
            err_msg = (
                'Invalid configuration for LookupTable;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for LookupTable;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['display_name'], repr_attrs=['_config', 'db_path'])
class FactConfig:
    def __init__(self, config: Dict, db_path: str) -> None:
        self.logger = module_logger.getChild('FactConfig')
        self._config = config  # type: Dict
        try:
            self.db_path = config.get('db_path', db_path)
            self.table_name = config['table_name']  # type: str
            self.display_name = config['display_name']  # type: str
            self.primary_key = PrimaryKeyConfig(config['primary_key'], self.table_name)
            self.editable = config.get('editable', False)  # type: bool
            self.show_on_load = config.get('show_on_load', True)  # type: bool
            self.display_rows = config.get('display_rows', 10000)  # type: int
            self.refresh_on_update = config.get('refresh_on_update', False)
            self.order_by = [
                OrderBy(field_name=ord_cfg['field_name'],
                        sort_order=SortOrder[ord_cfg['sort_order']])
                for ord_cfg in config.get('order_by', [])
            ]
            self.fields = [self.primary_key]
            self.fields += [
                FieldConfig(fld_cfg, self.table_name)
                for fld_cfg in config['fields']
            ]
            self.foreign_keys = [
                ForeignKeyConfig(fk_cfg, self.table_name)
                for fk_cfg in config.get('foreign_keys', [])
            ]
            self.calculated_fields = [
                CalculatedFieldConfig(calc_cfg, self.table_name)
                for calc_cfg in config.get('calculated_fields', [])
            ]
            self.validate()
        except (KeyError, AttributeError) as e:
            err_msg = (
                'Invalid configuration for Fact;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for Fact;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['aggregate_display_name', 'base_field_display_name',
                     'aggregate_func'],
          repr_attrs=['_config'])
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
            err_msg = (
                'Invalid configuration for AdditiveField;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for AdditiveField;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['view_display_name', 'fact_table_name'],
          repr_attrs=['_config'])
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
            err_msg = (
                'Invalid configuration for View;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for View;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@autorepr(str_attrs=['facts', 'dimensions', 'lookup_tables', 'views'],
          repr_attrs=['_config'])
class ConstellationConfig:
    def __init__(self, config: Dict) -> None:
        self.logger = module_logger.getChild('ConstellationConfig')
        self._config = config  # type: Dict
        try:
            self.app = AppConfig(config['app'])
            db_path = self.app.db_path
            self.dimensions = [
                DimensionConfig(dim_cfg, db_path)
                for dim_cfg in config.get('dimensions', [])
            ]
            self.lookup_tables = [
                LookupTableConfig(lkp_cfg, db_path)
                for lkp_cfg in config.get('lookup_tables', [])
            ]
            self.facts = [
                FactConfig(fact_cfg, db_path)
                for fact_cfg in config['facts']
            ]
            self.views = [
                ViewConfig(view_cfg)
                for view_cfg in config['views']
            ]
        except (KeyError, AttributeError) as e:
            err_msg = (
                'Invalid configuration for Constellation;'
                'missing a required field; error {}'
                .format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)
        except Exception as e:
            err_msg = (
                'Invalid configuration for Constellation;'
                'error {}'.format(str(e))
            )
            self.logger.error(err_msg)
            raise ConfigurationError(err_msg)

    def validate(self):
        pass


@lru_cache(maxsize=1)
def get_config(json_path: str=os.path.join('constellations', 'default.json')) -> Dict:
    with open(json_path) as fh:
        return json.load(fh)


if __name__ != '__main__':
    default_config = ConstellationConfig(get_config())

if __name__ == '__main__':
    from pathlib import Path
    dir = Path(__file__).parents[1]
    print('dir:', dir)
    path = os.path.join(str(dir), 'constellations', 'ireadgud.json')
    print(path)
    print('default_config:', ConstellationConfig(get_config(path)))

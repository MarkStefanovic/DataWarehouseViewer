from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import func


from schema.custom_types import FieldName, FieldType, FieldFormat
from schema.field import Field
from schema.utilities import autorepr, static_property


@autorepr
class AdditiveField:
    """A field that represents an aggregate of a Fact.

    This field type is only used with Views over a Star.
    It mimics its base field except for the schema and editability"""

    def __init__(self, *,
        base_field_display_name: FieldName,
        aggregate_display_name: FieldName,
        aggregate_func: func=func.sum
    ) -> None:

        self.base_field_display_name = base_field_display_name
        self.display_name = aggregate_display_name
        self.aggregate_func = aggregate_func

        # The star property is injected by the Star itself later.
        # It must be populated before this field can be used.
        self._star = None

        self.validate_config()

    @static_property
    def base_field(self) -> Field:
        if not self.star:
            raise AttributeError('AdditiveField {} must be assigned a star before use.'
                                 .format(self.display_name))
        try:
            return self.star.fields_by_display_name[self.base_field_display_name]
        except KeyError:
            print('Error creating additive field {}; '
                  'unable to find base field named {}'
                  .format(self.display_name, self.base_field_display_name))

    @static_property
    def dtype(self):
        """Mimic field property"""
        dtypes = {
            'count': FieldType.Int,
            'avg': FieldType.Float,
            'sum': FieldType.Float
        }
        try:
            return dtypes[self.sqa_func]
        except KeyError:
            print('Unable to find data type of AdditiveField {}'
                  .format(self.display_name))
            return self.base_field.dtype

    @static_property
    def editable(self):
        """Mimic field property"""
        return False

    @static_property
    def field_format(self):
        """Mimic field property"""
        if self.dtype == FieldType.Int:
            return FieldFormat.Int
        return FieldFormat.Accounting

    @static_property
    def filter_operators(self):
        """Mimic field property"""
        return self.base_field.filter_operators

    @static_property
    def name(self):
        """Mimic field property"""
        return self.base_field.name

    @static_property
    def primary_key(self):
        """Mimic field property"""
        return False

    @static_property
    def schema(self):
        try:
            return self.aggregate_func(self.base_field.schema)\
                   .cast(self.sqa_dtype).label(self.display_name)
        except Exception as e:
            print('error creating aggregate field {}; error: {}'
                  .format(self.display_name, str(e)))

    @static_property
    def sqa_dtype(self):
        lkp = {
            'avg': Float(14, 2),
            'count': Integer,
            'sum': Float(14, 2)
        }
        try:
            return lkp[self.sqa_func]
        except KeyError:
            print('Unable to find sqa_dtype for AdditiveField {} sqa_func {}'
                  .format(self.display_name, self.sqa_func))

    @static_property
    def sqa_func(self) -> str:
        try:
            return self.aggregate_func._FunctionGenerator__names[0]
        except KeyError:
            print('Error looking up sqa_func for AdditiveField {}'
                  .format(self.display_name))

    @static_property
    def star(self):
        if not self._star:
            raise(AttributeError, "The star for AdditiveField {} was not"
                                  "injected prior to calling the field."
                                  .format(self.display_name))
        return self._star

    def validate_config(self):
        if self.sqa_func not in ['avg', 'count', 'sum']:
            from schema.config import ConfigError
            raise ConfigError('The function {} for AdditiveField {} is not implemented.'
                              .format(self.sqa_func, self.display_name))

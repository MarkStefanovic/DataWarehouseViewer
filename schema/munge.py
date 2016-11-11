import datetime

from typing import Optional
import re
from schema.custom_types import (
    FieldType, 
    FieldFormat,
    SqlDataType
)

date_str_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}.*$")


def convert_value(*,
        field_type: FieldType,
        value: Optional[SqlDataType]=None
    ) -> SqlDataType:
    """Convert a string value to a Python data type

    This conversion function is used to translate user input to a form that
    sqlalchemy can use."""
    def convert_date(date_val: Optional[str]='1900-01-01'):
        try:
            if not date_val:
                return datetime.datetime(1900, 1, 1).date()
            if isinstance(date_val, str):
                if date_str_pattern.match(date_val):
                    return datetime.datetime.strptime(date_val[:10], "%Y-%m-%d").date()
                raise ValueError("{v} is not a valid date".format(v=date_val))
            elif isinstance(date_val, datetime.date):
                return date_val
            return '' #datetime.datetime(1900, 1, 1).date()
        except Exception as e:
            print("Error converting date value {} to a date; "
                  "the current type is {}; error {}"\
                  .format(date_val, type(date_val), str(e)))

    conversion_functions = {
        FieldType.Date:  convert_date,
        FieldType.Float: lambda v: round(float(v), 2),
        FieldType.Int:   int,
        FieldType.Str:   str,
        FieldType.Bool:  bool
    }
    # default_values = {
    #     FieldType.Date:  None,
    #     FieldType.Float: 0.0,
    #     FieldType.Int:   '',
    #     FieldType.Str:   '',
    #     FieldType.Bool:  False
    # }
    try:
        if not value:
            # We need to return None instead of an empty string or 0 in the
            # case of a date field.
            return None #return default_values[field_type]
        return conversion_functions[field_type](value)
    except Exception as e:
        print('Error converting value {} to data type {}; err:'
              .format(value, field_type, str(e)))
        return None #default_values[field_type]


def format_value(*,
        field_type: FieldType,
        value: Optional[SqlDataType]=None,
        field_format: Optional[FieldFormat]=None
    ) -> SqlDataType:
    """Format a string value to a string appropriate for display to the user"""

    inferred_data_types = {
        FieldFormat.Accounting: FieldType.Float,
        FieldFormat.Bool:       FieldType.Bool,
        FieldFormat.Currency:   FieldType.Float,
        FieldFormat.Date:       FieldType.Date,
        FieldFormat.DateTime:   FieldType.Date,
        FieldFormat.Float:      FieldType.Float,
        FieldFormat.Int:        FieldType.Int,
        FieldFormat.Str:        FieldType.Str
    }
    data_type = inferred_data_types[field_format] if not field_type else field_type
    inferred_format = lambda fld_type: next(k for k, v in inferred_data_types.items() if v == field_type)
    format = inferred_format(field_type) if not field_format else field_format
    formatters = {
        FieldFormat.Accounting: lambda val: '{: ,.2f} '.format(round(val, 2)),
        FieldFormat.Bool:       lambda val: str(val),
        FieldFormat.Currency:   lambda val: '${: ,.2f} '.format(round(val, 2)),
        FieldFormat.Date:       lambda val: str(val),
        FieldFormat.DateTime:   lambda val: str(val),
        FieldFormat.Float:      lambda val: '{:,.4f}'.format(round(val, 2)),
        FieldFormat.Int:        lambda val: '{: d}'.format(round(val, 2)),
        FieldFormat.Str:        lambda val: val
    }
    try:
        if value:
            converted_val = convert_value(field_type=data_type, value=value)
            return formatters[format](converted_val)
        return None #default_display_values[data_type]
    except Exception as e:
        print(
            'error formatting value,',
            'val:', value,
            'data_type:', data_type,
            'error msg:', str(e)
        )
        return value

import sqlalchemy as sqa

from schema.custom_types import DimensionName, FieldType
from schema.field import Field
from schema.utilities import autorepr, static_property


@autorepr
class ForeignKey(Field):
    def __init__(self, *,
            name: str,
            display_name: str,
            dimension: DimensionName,
            foreign_key_field: str
    ) -> None:

        super(ForeignKey, self).__init__(
            name=name,
            dtype=FieldType.Int,
            display_name=display_name,
            filter_operators=None,
            editable=True,
            primary_key=False
        )

        self.dimension = dimension
        self.foreign_key_field = foreign_key_field  # name of id field on dim

    @static_property
    def schema(self) -> sqa.Column:
        return sqa.Column(
            self.name,
            None,
            sqa.ForeignKey("{t}.{f}".format(t=self.dimension,
                                            f=self.foreign_key_field))
        )

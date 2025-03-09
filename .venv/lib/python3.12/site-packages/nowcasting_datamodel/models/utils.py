""" General Models"""

from datetime import datetime

from pydantic import BaseModel
from sqlalchemy import Column, DateTime

from nowcasting_datamodel.utils import convert_to_camelcase


########
# 1. Reusable classes
########
class CreatedMixin:
    """Mixin to add created datetime to model"""

    created_utc = Column(DateTime(timezone=True), default=lambda: datetime.utcnow())


class EnhancedBaseModel(BaseModel):
    """Ensures that attribute names are returned in camelCase"""

    # Automatically creates camelcase alias for field names
    # See https://pydantic-docs.helpmanual.io/usage/model_config/#alias-generator
    class Config:  # noqa: D106
        alias_generator = convert_to_camelcase
        allow_population_by_field_name = True
        orm_mode = True
        underscore_attrs_are_private = True
        from_attributes = True
        populate_by_name = True

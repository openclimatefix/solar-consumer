""" Pydantic and Sqlalchemy models for the database

The following class are made
1. Reusable classes
3. Model object, what forecast model is used
4. ForecastValue objects, specific values of a forecast and time
5. Input data status, shows when the data was collected
6. Forecasts, a forecast that is made for one gsp, for several time steps into the future

"""

import logging
from datetime import datetime
from typing import Optional

from pydantic import Field, field_validator
from sqlalchemy import Column, DateTime, Index, Integer, String
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_with_timezone

national_gb_label = "National-GB"

logger = logging.getLogger(__name__)
# TODO #3 Add forecast latest table, this make it easy to load the latest forecast


########
# 3. Model
########
class MLModelSQL(Base_Forecast):
    """ML model that is being used"""

    __tablename__ = "model"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    version = Column(String)

    forecast = relationship("ForecastSQL", back_populates="model")
    metric_value = relationship("MetricValueSQL", back_populates="model")


class MLModel(EnhancedBaseModel):
    """ML model that is being used"""

    name: Optional[str] = Field(..., description="The name of the model", index=True)
    version: Optional[str] = Field(..., description="The version of the model")

    def to_orm(self) -> MLModelSQL:
        """Change model to MLModelSQL"""
        return MLModelSQL(
            name=self.name,
            version=self.version,
        )


########
# 4. ForecastValue
# A. SQL table saves all forecast values
# B. SQL table that save the latest forecast
# C. Pydantic object
########


########
# 5. Input data status
########
class InputDataLastUpdatedSQL(Base_Forecast, CreatedMixin):
    """Information about the input data that was used to create the forecast"""

    __tablename__ = "input_data_last_updated"

    id = Column(Integer, primary_key=True)
    gsp = Column(DateTime(timezone=True))
    nwp = Column(DateTime(timezone=True))
    pv = Column(DateTime(timezone=True))
    satellite = Column(DateTime(timezone=True))

    forecast = relationship("ForecastSQL", back_populates="input_data_last_updated")

    Index("index_input_data", CreatedMixin.created_utc.desc())


class InputDataLastUpdated(EnhancedBaseModel):
    """Information about the input data that was used to create the forecast"""

    gsp: datetime = Field(..., description="The time when the input GSP data was last updated")
    nwp: datetime = Field(..., description="The time when the input NWP data was last updated")
    pv: datetime = Field(..., description="The time when the input PV data was last updated")
    satellite: datetime = Field(
        ..., description="The time when the input satellite data was last updated"
    )

    @field_validator("gsp", mode="before")
    def normalize_gsp(cls, v):
        """Normalize gsp field"""
        return datetime_with_timezone(cls, v)

    @field_validator("nwp", mode="before")
    def normalize_nwp(cls, v):
        """Normalize nwp field"""
        return datetime_with_timezone(cls, v)

    @field_validator("pv", mode="before")
    def normalize_pv(cls, v):
        """Normalize pv field"""
        return datetime_with_timezone(cls, v)

    @field_validator("satellite", mode="before")
    def normalize_satellite(cls, v):
        """Normalize satellite field"""
        return datetime_with_timezone(cls, v)

    def to_orm(self) -> InputDataLastUpdatedSQL:
        """Change model to InputDataLastUpdatedSQL"""
        return InputDataLastUpdatedSQL(
            gsp=self.gsp,
            nwp=self.nwp,
            pv=self.pv,
            satellite=self.satellite,
        )


########
# 7. Status
########


class StatusSQL(Base_Forecast, CreatedMixin):
    """Status SQL Model"""

    __tablename__ = "status"

    id = Column(Integer, primary_key=True)
    status = Column(String)
    message = Column(String)


class Status(EnhancedBaseModel):
    """Status Model for a single message"""

    status: str = Field(..., description="Status description")
    message: str = Field(..., description="Status Message")

    def to_orm(self) -> StatusSQL:
        """Change model to SQL"""
        return StatusSQL(status=self.status, message=self.message)

    @field_validator("status")
    def validate_solar_generation_kw(cls, v):
        """Validate the solar_generation_kw field"""
        if v not in ["ok", "warning", "error"]:
            message = f"Status is {v}, not one of 'ok', 'warning', 'error'"
            logger.debug(message)
            raise Exception(message)
        return v

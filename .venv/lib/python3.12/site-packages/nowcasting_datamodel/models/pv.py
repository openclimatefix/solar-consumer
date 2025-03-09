""" Pydantic and Sqlalchemy models for the database

7. PV system for storing PV data (pv.py)
8. PV yield for storing PV data (pv.py)

"""

import logging
from datetime import datetime
from typing import Optional

from pydantic import Field, field_validator
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models.base import Base_PV
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_with_timezone

logger = logging.getLogger(__name__)
pv_output = "pvoutput.org"
solar_sheffield_passiv = "solar_sheffield_passiv"
providers = [pv_output, solar_sheffield_passiv]

########
# 7. PV Metadata
########


class PVSystemSQL(Base_PV, CreatedMixin):
    """Metadata for PV data"""

    __tablename__ = "pv_system"

    id = Column(Integer, primary_key=True)
    pv_system_id = Column(Integer, index=True)
    provider = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    name = Column(String, nullable=True)
    orientation = Column(Float, nullable=True)
    status_interval_minutes = Column(Integer, nullable=True)
    installed_capacity_kw = Column(
        Float,
        nullable=True,
    )
    ml_capacity_kw = Column(
        Float,
        nullable=True,
    )
    ocf_id = Column(Integer, nullable=True)
    correct_data = Column(Boolean, default=True)

    pv_yield = relationship("PVYieldSQL", back_populates="pv_system")


class PVSystem(EnhancedBaseModel):
    """Metadata for PV data"""

    pv_system_id: int = Field(..., description="The PV system id")
    provider: str = Field(..., description="The provider of the PV system")
    latitude: float = Field(None, description="The latitude of the PV system")
    longitude: float = Field(None, description="The longitude of the PV system")
    name: Optional[str] = Field(None, description="The PV system name")
    orientation: Optional[float] = Field(None, description="The orientation of the PV system")
    status_interval_minutes: Optional[float] = Field(
        None, description="The number of minutes for the pv data to be refreshed"
    )
    installed_capacity_kw: Optional[float] = Field(
        None, description="The capacity of the pv system in kw."
    )
    ml_capacity_kw: Optional[float] = Field(
        None,
        description="The capacity of the pv system in kw, user for ML models. "
        "This may be different from the installed capacity",
    )
    correct_data: Optional[bool] = Field(
        True, description="If the data from the pv system is not broken in some way"
    )
    ocf_id: Optional[int] = Field(None, description="The PV system id that is unique to OCF")

    @field_validator("provider")
    def validate_provider(cls, v):
        """Validate the provider"""
        if v not in providers:
            raise Exception(f"Provider ({v}) must be in {providers}")
        return v

    def to_orm(self) -> PVSystemSQL:
        """Change model to PVSystemSQL"""
        return PVSystemSQL(
            pv_system_id=self.pv_system_id,
            provider=self.provider,
            latitude=self.latitude,
            longitude=self.longitude,
            name=self.name,
            orientation=self.orientation,
            status_interval_minutes=self.status_interval_minutes,
            correct_data=self.correct_data,
            installed_capacity_kw=self.installed_capacity_kw,
            ml_capacity_kw=self.ml_capacity_kw,
            ocf_id=self.ocf_id,
        )


########
# 8. PV Yield
########


class PVYieldSQL(Base_PV, CreatedMixin):
    """PV Yield data"""

    __tablename__ = "pv_yield"

    id = Column(Integer, primary_key=True)
    datetime_utc = Column(DateTime, index=True)
    solar_generation_kw = Column(Float)

    # many (forecasts) to one (location)
    pv_system = relationship("PVSystemSQL", back_populates="pv_yield")
    pv_system_id = Column(Integer, ForeignKey("pv_system.id"), index=True)

    Index("ix_datetime_utc", datetime_utc.desc())


class PVYield(EnhancedBaseModel):
    """PV Yield data"""

    datetime_utc: datetime = Field(..., description="The timestamp of the pv system")
    solar_generation_kw: float = Field(..., description="The provider of the PV system")
    pv_system: Optional[PVSystem] = Field(
        None,
        description="The PV system associated with this model",
    )

    @field_validator("datetime_utc", mode="before")
    def normalize_datetime_utc(cls, v):
        """Normalize datetime_utc field"""
        return datetime_with_timezone(cls, v)

    @field_validator("solar_generation_kw")
    def validate_solar_generation_kw(cls, v):
        """Validate the solar_generation_kw field"""
        if v < 0:
            logger.debug(f"Changing solar_generation_kw ({v}) to 0")
            v = 0
        return v

    def to_orm(self) -> PVYieldSQL:
        """Change model to PVYieldSQL"""
        return PVYieldSQL(
            datetime_utc=self.datetime_utc,
            solar_generation_kw=self.solar_generation_kw,
        )

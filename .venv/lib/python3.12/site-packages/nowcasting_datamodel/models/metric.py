"""
The followin tables are made with sqlamyc and pydantic

1. metric, stores the metric metadata
2. datetime_interval - The datetime interval that the metric is for
3. metric_value, the actual value of this metric

"""

from datetime import datetime, time
from typing import ClassVar, Optional

from pydantic import Field, field_validator
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Index, Integer, String, Time
from sqlalchemy.orm import relationship

from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models.gsp import Location
from nowcasting_datamodel.models.utils import CreatedMixin, EnhancedBaseModel
from nowcasting_datamodel.utils import datetime_with_timezone

########
# 1. Metric
########


class MetricSQL(Base_Forecast):
    """Metric metadata"""

    __tablename__ = "metric"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)

    metric_value = relationship("MetricValueSQL", back_populates="metric")


class Metric(EnhancedBaseModel):
    """Metric metadata"""

    name: str = Field(..., description="The name of the metric")
    description: str = Field(..., description="The description of the metric")

    rm_mode: ClassVar[bool] = True

    def to_orm(self) -> MetricSQL:
        """Change model to LocationSQL"""

        if isinstance(self, MetricSQL):
            return self

        return MetricSQL(
            name=self.name,
            description=self.description,
        )


########
# 2. Datetime Interval
########


class DatetimeIntervalSQL(Base_Forecast):
    """Datetime interval data"""

    __tablename__ = "datetime_interval"

    # add a unique condition on 'gsp_id' and 'target_time'
    __table_args__ = (
        Index(
            "uix_2",  # Index name
            "start_datetime_utc",
            "end_datetime_utc",  # Columns which are part of the index
            unique=True,
            postgresql_where=Column("is_primary"),  # The condition
        ),
    )

    id = Column(Integer, primary_key=True)
    start_datetime_utc = Column(DateTime, index=True)
    end_datetime_utc = Column(DateTime, index=True)
    elexon_settlement_period = Column(Integer, nullable=True)
    is_primary = Column(Boolean, default=True)

    metric_value = relationship("MetricValueSQL", back_populates="datetime_interval")

    Index("ix_start_datetime_utc", start_datetime_utc.desc())
    Index("ix_end_datetime_utc", end_datetime_utc.desc())

    # add unique on start and end datetime


class DatetimeInterval(EnhancedBaseModel):
    """Datetime interval data"""

    start_datetime_utc: datetime = Field(
        ..., description="The timestamp of the start of the interval"
    )
    end_datetime_utc: datetime = Field(..., description="The timestamp of the end of the interval")
    elexon_settlement_period: Optional[datetime] = Field(
        None, description="The elexon settlement period. This is optional"
    )

    @field_validator("start_datetime_utc", mode="before")
    def normalize_start_datetime_utc(cls, v):
        """Normalize start_datetime_utc field"""
        return datetime_with_timezone(cls, v)

    @field_validator("end_datetime_utc", mode="before")
    def normalize_end_datetime_utc(cls, v):
        """Normalize end_datetime_utc field"""
        return datetime_with_timezone(cls, v)

    def to_orm(self) -> DatetimeIntervalSQL:
        """Change model to GSPYieldSQL"""
        return DatetimeIntervalSQL(
            start_datetime_utc=self.start_datetime_utc,
            end_datetime_utc=self.end_datetime_utc,
            elexon_settlement_period=self.elexon_settlement_period,
        )


########
# 3. Metirc Value
########


class MetricValueSQL(Base_Forecast, CreatedMixin):
    """The calculated metric value"""

    __tablename__ = "metric_value"

    id = Column(Integer, primary_key=True)
    value = Column(Float, nullable=False)
    number_of_data_points = Column(Integer, nullable=False)
    forecast_horizon_minutes = Column(Integer, nullable=True)
    time_of_day = Column(Time, nullable=True)
    model_name = Column(String, nullable=True)
    p_level = Column(Float, nullable=True)

    # many (metric values) to one (metric)
    metric = relationship("MetricSQL", back_populates="metric_value")
    metric_id = Column(Integer, ForeignKey("metric.id"), index=True)

    # many (metric values) to one (location)
    location = relationship("LocationSQL", back_populates="metric_value")
    location_id = Column(Integer, ForeignKey("location.id"), index=True)

    # many (metric values) to one (location)
    datetime_interval = relationship("DatetimeIntervalSQL", back_populates="metric_value")
    datetime_interval_id = Column(Integer, ForeignKey("datetime_interval.id"), index=True)

    # many (metric values) to one (model)
    model = relationship("MLModelSQL", back_populates="metric_value")
    model_id = Column(Integer, ForeignKey("model.id"), index=True)


class MetricValue(EnhancedBaseModel):
    """Location that the forecast is for"""

    value: float = Field(..., description="The value of the metric")
    number_of_data_points: int = Field(
        ..., description="The number of data points used to make the value"
    )
    forecast_horizon_minutes: int = Field(
        None,
        description="The forecast horizon in minutes. "
        "60 minutes means the forecast made 60 mintues before the target time",
    )
    time_of_day: Optional[time] = Field(None, description="the time of tday for this metric")
    metric: Metric = Field(..., description="The metric this value is about")
    datetime_interval: DatetimeInterval = Field(
        ..., description="The datetime interval this value is about"
    )
    location: Location = Field(None, description="The location object for this metric value")

    rm_mode: ClassVar[bool] = True

    def to_orm(self) -> MetricValueSQL:
        """Change model to MetricValueSQL"""

        if isinstance(self, MetricValueSQL):
            return self

        return MetricValueSQL(
            value=self.value,
            number_of_data_points=self.number_of_data_points,
            metric=self.metric.to_orm(),
            location=self.location.to_orm(),
            datetime_interval=self.datetime_interval.to_orm(),
            forecast_horizon_minutes=self.forecast_horizon_minutes,
            time_of_day=self.time_of_day,
        )

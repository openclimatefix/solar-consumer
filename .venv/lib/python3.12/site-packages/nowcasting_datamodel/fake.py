""" Functions used to make fake forecasts"""

from datetime import datetime, time, timedelta, timezone
from typing import List, Optional

import numpy as np
from sqlalchemy.orm import Session

from nowcasting_datamodel.models import (
    InputDataLastUpdatedSQL,
    LocationSQL,
    MetricSQL,
    MetricValueSQL,
    MLModelSQL,
    PVSystemSQL,
    national_gb_label,
)
from nowcasting_datamodel.models.forecast import ForecastSQL, ForecastValueSQL
from nowcasting_datamodel.models.gsp import GSPYieldSQL
from nowcasting_datamodel.read.read import get_location
from nowcasting_datamodel.read.read_metric import get_datetime_interval
from nowcasting_datamodel.read.read_models import get_model
from nowcasting_datamodel.save.update import change_forecast_value_to_latest

# 2 days in the past + 8 hours forward at 30 mins interval
N_FAKE_FORECASTS = (24 * 2 + 8) * 2
TOTAL_MINUTES_IN_ONE_DAY = 24 * 60


def make_fake_location(gsp_id: int) -> LocationSQL:
    """Make fake location with gsp id"""
    return LocationSQL(label=f"GSP_{gsp_id}", gsp_id=gsp_id)


def make_fake_pv_system() -> PVSystemSQL:
    """Make fake location with gsp id"""
    return PVSystemSQL(pv_system_id=1, provider="pvoutput.org", latitude=55, longitude=0)


def make_fake_input_data_last_updated() -> InputDataLastUpdatedSQL:
    """Make fake input data last updated"""
    now = datetime.now(tz=timezone.utc)

    return InputDataLastUpdatedSQL(gsp=now, nwp=now, pv=now, satellite=now)


def make_fake_forecast_value(
    target_time, forecast_maximum: Optional[int] = 1, random_factor: Optional[float] = 1
) -> ForecastValueSQL:
    """Make fake forecast value"""

    intensity = make_fake_intensity(target_time)
    power = forecast_maximum * intensity * random_factor

    return ForecastValueSQL(
        target_time=target_time,
        expected_power_generation_megawatts=power,
        adjust_mw=0.0,
        properties={"10": power * 0.9, "90": power * 1.1},
    )


def make_fake_forecast(
    gsp_id: int,
    session: Session,
    t0_datetime_utc: Optional[datetime] = None,
    forecast_values: Optional[List[ForecastValueSQL]] = None,
    forecast_values_latest: Optional[List[ForecastValueSQL]] = None,
    add_latest: Optional[bool] = False,
    historic: Optional[bool] = False,
    model_name: Optional[str] = "fake_model",
    n_fake_forecasts: Optional[int] = N_FAKE_FORECASTS,
) -> ForecastSQL:
    """Make one fake forecast"""

    if gsp_id == 0:
        # national capacity
        installed_capacity_mw = 13000
    else:
        # gsp capacity (roughly)
        installed_capacity_mw = 10

    location = get_location(
        gsp_id=gsp_id, session=session, installed_capacity_mw=installed_capacity_mw
    )

    model = get_model(name=model_name, session=session, version="0.1.2")
    input_data_last_updated = make_fake_input_data_last_updated()

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2024, 1, 1, tzinfo=timezone.utc)

    random_factor = 0.9 + 0.1 * np.random.random()

    # create
    if forecast_values is None:
        forecast_values = []
        # 2 days in the past + 8 hours forward
        for i in range(n_fake_forecasts):
            target_datetime_utc = t0_datetime_utc + timedelta(minutes=i * 30) - timedelta(days=2)
            f = make_fake_forecast_value(
                target_time=target_datetime_utc,
                forecast_maximum=location.installed_capacity_mw,
                random_factor=random_factor,
            )
            forecast_values.append(f)

    if forecast_values_latest is None:
        forecast_values_latest = []

        if add_latest:
            for forecast_value in forecast_values:
                forecast_values_latest.append(
                    change_forecast_value_to_latest(
                        forecast_value, gsp_id=location.gsp_id, model_id=model.id
                    )
                )

    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=t0_datetime_utc,
        location=location,
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
        forecast_values_latest=forecast_values_latest,
        historic=historic,
    )

    return forecast


def make_fake_forecasts(
    gsp_ids: List[int],
    session: Session,
    t0_datetime_utc: Optional[datetime] = None,
    forecast_values: Optional[List[ForecastValueSQL]] = None,
    add_latest: Optional[bool] = False,
    historic: Optional[bool] = False,
    model_name: Optional[str] = "fake_model",
    n_fake_forecasts: Optional[int] = N_FAKE_FORECASTS,
) -> List[ForecastSQL]:
    """Make many fake forecast"""
    forecasts = []
    for gsp_id in gsp_ids:
        forecasts.append(
            make_fake_forecast(
                gsp_id=gsp_id,
                t0_datetime_utc=t0_datetime_utc,
                session=session,
                forecast_values=forecast_values,
                add_latest=add_latest,
                historic=historic,
                model_name=model_name,
                n_fake_forecasts=n_fake_forecasts,
            )
        )

    session.add_all(forecasts)

    return forecasts


def make_fake_national_forecast(
    session: Session, t0_datetime_utc: Optional[datetime] = None
) -> ForecastSQL:
    """Make national fake forecast"""
    location = get_location(
        gsp_id=0, session=session, label=national_gb_label, installed_capacity_mw=14000
    )
    model = MLModelSQL(name="fake_model_national", version="0.1.2")
    input_data_last_updated = make_fake_input_data_last_updated()

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2024, 1, 1, tzinfo=timezone.utc)

    random_factor = 0.9 + 0.1 * np.random.random()

    # create
    forecast_values = []
    for i in range(N_FAKE_FORECASTS):
        target_datetime_utc = t0_datetime_utc + timedelta(minutes=i * 30) - timedelta(days=2)
        f = make_fake_forecast_value(target_time=target_datetime_utc, random_factor=random_factor)
        forecast_values.append(f)

    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=t0_datetime_utc,
        location=location,
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
        historic=False,
    )

    return forecast


def make_fake_gsp_yields_for_one_location(
    gsp_id: int, session: Session, t0_datetime_utc: Optional[datetime] = None
):
    """
    Make GSP yields for one locations

    :param gsp_id: the gsp id you want gsp yields for
    :param session: database sessions
    :param t0_datetime_utc: the time now.
    """

    if gsp_id == 0:
        # national capacity
        installed_capacity_mw = 13000
    else:
        # gsp capacity (roughly)
        installed_capacity_mw = 10

    if t0_datetime_utc is None:
        t0_datetime_utc = datetime(2024, 1, 1, tzinfo=timezone.utc)

    location = get_location(
        session=session, gsp_id=gsp_id, installed_capacity_mw=installed_capacity_mw
    )

    random_factor = 0.9 + 0.1 * np.random.random()

    # make 2 days of fake data
    for i in range(48 * 2):
        datetime_utc = t0_datetime_utc - timedelta(days=2) + timedelta(minutes=i * 30)

        intensity = make_fake_intensity(datetime_utc)
        power = installed_capacity_mw * intensity * random_factor

        gsp_yield = GSPYieldSQL(
            datetime_utc=datetime_utc,
            solar_generation_kw=power,
            regime="in-day",
        )
        gsp_yield.location = location

        session.add(gsp_yield)

        gsp_yield = GSPYieldSQL(
            datetime_utc=datetime_utc, solar_generation_kw=3, regime="day-after"
        )
        gsp_yield.location = location

        session.add(gsp_yield)


def make_fake_intensity(datetime_utc: datetime) -> float:
    """
    Make a fake intesnity value based on the time of the day

    :param datetime_utc:
    :return: inteisty, between 0 and 1
    """
    fraction_of_day = (datetime_utc.hour * 60 + datetime_utc.minute) / TOTAL_MINUTES_IN_ONE_DAY
    # use single cos**2 wave for intensity, but set night time to zero
    if (fraction_of_day > 0.25) & (fraction_of_day < 0.75):
        intensity = np.cos(2 * np.pi * fraction_of_day) ** 2
    else:
        intensity = 0.0
    return float(intensity)


def make_fake_gsp_yields(
    session: Session, gsp_ids: List[int], t0_datetime_utc: Optional[datetime] = None
):
    """
    Make GSP yields for many locations

    :param gsp_ids: the gsp ids you want gsp yields for
    :param session: database sessions
    :param t0_datetime_utc: the time now.
    """

    for gsp_id in gsp_ids:
        make_fake_gsp_yields_for_one_location(
            gsp_id=gsp_id, session=session, t0_datetime_utc=t0_datetime_utc
        )


def make_fake_me_latest(session: Session, model_name: str = "fake_model"):
    """Make fake ME latest objects"""
    # create
    metric = MetricSQL(name="Half Hourly ME", description="test")
    datetime_interval = get_datetime_interval(
        session=session,
        start_datetime_utc=datetime(2023, 1, 1),
        end_datetime_utc=datetime(2023, 1, 8),
    )
    location = get_location(
        gsp_id=0, session=session, installed_capacity_mw=14000, label=national_gb_label
    )
    model = MLModelSQL(name=model_name, version="0.1.2")

    session.add_all([location, datetime_interval, metric])

    metric_values = []
    for forecast_horizon in range(0, 60 * 9, 30):
        for minutes in range(0, 60 * 24, 30):
            time_of_day = time(hour=minutes // 60, minute=minutes % 60)
            m = MetricValueSQL(
                value=forecast_horizon * 10000 + minutes,
                time_of_day=time_of_day,
                forecast_horizon_minutes=forecast_horizon,
                number_of_data_points=1,
                datetime_interval=datetime_interval,
                metric=metric,
                location=location,
                model_name=model_name,
                model=model,
            )
            metric_values.append(m)

    return metric_values

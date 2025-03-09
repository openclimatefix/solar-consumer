""" Methods for adding adjust values to the forecast"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Union

import numpy as np
import pandas as pd

from nowcasting_datamodel.models import Forecast, ForecastSQL, MetricValue, MetricValueSQL
from nowcasting_datamodel.read.read_metric import read_latest_me_national

logger = logging.getLogger()

MAX_ADJUST_PER = 0.2


def add_adjust_to_forecasts(
    forecasts_sql: List[ForecastSQL],
    session,
    max_adjust_percentage: Optional[float] = MAX_ADJUST_PER,
):
    """
    Adjust National Forecast by ME over the last week

    Note that no objects are returned as sqlalchemy objects can be adjust on the spot

    :param forecasts_sql: list of forecasts
    :param session: database sessions
    :param max_adjust_percentage: maximum percentage of forecast value that can be adjusted.
        If this is None, then no limit is used
    """
    logger.debug("Adding Adjusts to National forecast")

    # get the national forecast only
    forecast_national = [f for f in forecasts_sql if f.location.gsp_id == 0]

    if len(forecast_national) != 1:
        logger.debug("Could not find single national forecast, tehre fore not adding adjust")

    add_adjust_to_national_forecast(
        forecast=forecast_national[0], session=session, max_adjust_percentage=max_adjust_percentage
    )


def add_adjust_to_national_forecast(
    forecast: ForecastSQL, session, max_adjust_percentage: Optional[float] = MAX_ADJUST_PER
):
    """
    Add adjust to national forecast.

    1. Get latest me results for time_of_day and forecast horizont
    2. Reduce latest me results relevant for now
    3. Reduce adjust value by at most X% of forecast_value.expected_power_generation_megawatts,
    4. Add Me to adjust_mw property in ForecastValueSQL

    :param forecast: national forecast
    :param session:
    :param max_adjust_percentage: maximum percentage of forecast value that can be adjusted.
        If this is None, then no limit is used

    :return:
    """

    # get the target time and model name
    datetime_now = forecast.forecast_values[0].target_time
    last_datetime = forecast.forecast_values[-1].target_time
    model_name = forecast.model.name

    logger.debug(
        f"Adding adjuster to national forecast for "
        f"{datetime_now} and {model_name}, {last_datetime=}"
    )

    # 1. read metric values
    latest_me = read_latest_me_national(session=session, model_name=model_name)
    if len(latest_me) == 0:
        logger.warning(f"Found no ME values found for {model_name=}")
    else:
        logger.debug(f"Found {len(latest_me)} latest ME values")

    # 2. filter value down to now onwards
    # get the number of hours to go ahead, we've added 1 to make sure we use the last one as well
    hours_ahead = get_forecast_horizon_from_forecast(forecast)
    # change to dataframe
    latest_me_df = reduce_metric_values_to_correct_forecast_horizon(
        latest_me=latest_me, datetime_now=datetime_now, hours_ahead=hours_ahead
    )
    assert len(latest_me_df) > 0

    # 3. add values to forecast_values
    for forecast_value in forecast.forecast_values:
        target_time = forecast_value.target_time
        try:
            # get value from df
            value = latest_me_df[latest_me_df["datetime"] == forecast_value.target_time]
            value = value.iloc[0].value

            # reduce adjust value by at most 20% of expected_power_generation_megawatts,
            # if the forecast_value is not 0
            # note that adjust value can be negative, so we need to be careful
            # also, if the forecast value is 0, then adjust value should also be 0
            if max_adjust_percentage is not None:
                if forecast_value.expected_power_generation_megawatts != 0:
                    max_adjust = (
                        max_adjust_percentage * forecast_value.expected_power_generation_megawatts
                    )
                    if value > max_adjust:
                        value = max_adjust
                    elif value < -max_adjust:
                        value = -max_adjust
                else:
                    value = 0.0

            # add value to ForecastValueSQL
            forecast_value.adjust_mw = float(value)
            if np.isnan(value):
                logger.debug(
                    f"Found ME value for {target_time} in {latest_me_df}, "
                    f"but it was NaN, therefore adding adjust_mw as 0"
                )
                forecast_value.adjust_mw = 0.0

        except Exception:
            logger.debug(
                f"Could not find ME value for {target_time} in {latest_me_df}, "
                f"therefore adding adjust_mw as 0"
            )
            forecast_value.adjust_mw = 0.0


def get_forecast_horizon_from_forecast(forecast: Union[ForecastSQL, Forecast]):
    """
    Get the number of hours ahead from the forecast

    :param forecast:
    :return:
    """
    datetime_now = forecast.forecast_values[0].target_time
    last_datetime = forecast.forecast_values[-1].target_time
    hours_ahead = int((last_datetime - datetime_now).seconds / 3600) + 1
    hours_ahead += (last_datetime - datetime_now).days * 24

    logger.debug(f"Hours ahead is {hours_ahead}, {last_datetime=}, {datetime_now=} ")

    return hours_ahead


def reduce_metric_values_to_correct_forecast_horizon(
    latest_me: List[MetricValueSQL], datetime_now: datetime, hours_ahead=4
) -> pd.DataFrame:
    """
    Change all latest ME results 2D to 1D array

    Start:
    # time forecast_horizon value
    # 00.00 0  0.1
    # 00.00 30 0.2
    # 00.00 60 0.6
    # .....
    # 00.30 0  0.11
    # 00.30 30 0.21
    # 00.30 60 0.61
    # .....
    # 23.30 0  0.11
    # 23.30 30 0.21
    # 23.30 60 0.61

    Using datetime_now of 04:30

    End:
    # time forecast_horizon value
    # 04.30 0  0.1
    # 05.00 30 0.2
    # 05.30 60 0.6
    # .....

    :param latest_me: list of latest me results for all time_of_day and all forecast_horizons
    :param datetime_now: the dateteim now, so we know where to use forecast_horizons of 0
    :param hours_ahead: how many hours ahead
    :return: dataframe containing
        'datetime'
        'time_of_day'
        'forecast_horizon'
        'value'
    """

    logger.debug(
        f"Reducing metric values to correct forecast horizon {datetime_now=} {hours_ahead=}"
    )

    latest_me_df = pd.DataFrame(
        [MetricValue.model_validate(m, from_attributes=True).model_dump() for m in latest_me]
    )
    if len(latest_me_df) == 0:
        # no latest ME values, so just making an empty dataframe
        latest_me_df = pd.DataFrame(columns=["forecast_horizon_minutes", "time_of_day", "value"])

    # Let now big a dataframe of datetimes from now onwards. Lets say the time is 04.30, then
    # time forecast_horizon value
    # 04.30 0  0.1
    # 05.00 30 0.2
    # 05.30 60 0.6
    # .....

    results_df = pd.DataFrame(
        index=pd.date_range(
            start=datetime_now, end=datetime_now + timedelta(hours=hours_ahead), freq="30T"
        ),
    )
    results_df["datetime"] = results_df.index
    results_df["time_of_day"] = results_df.index.time
    results_df["forecast_horizon_minutes"] = range(0, 30 * len(results_df), 30)
    latest_me_df["forecast_horizon_minutes"] = latest_me_df["forecast_horizon_minutes"].astype(int)
    results_df["forecast_horizon_minutes"] = results_df["forecast_horizon_minutes"].astype(int)
    latest_me_df["value"] = latest_me_df["value"].astype(float)

    all_df = pd.merge(
        results_df,
        latest_me_df,
        how="left",
        on=["time_of_day", "forecast_horizon_minutes"],
    )

    all_df = all_df[["datetime", "time_of_day", "value", "forecast_horizon_minutes"]]

    return all_df

""" Functions to convert objects """

import logging
from datetime import datetime, timezone
from typing import List

import pandas as pd
from sqlalchemy.orm import Session

from nowcasting_datamodel.models import (
    Forecast,
    ForecastSQL,
    ForecastValue,
    ForecastValueSevenDaysSQL,
)
from nowcasting_datamodel.read.read import (
    get_latest_input_data_last_updated,
    get_location,
)
from nowcasting_datamodel.read.read_models import get_model

logger = logging.getLogger()


def convert_list_forecast_value_seven_days_sql_to_list_forecast(
    forecast_values_sql=List[ForecastValueSevenDaysSQL],
) -> List[Forecast]:
    """
    Convert a list of sql ForecastValueSevenDaysSQL to pydantic Forecast objects

    :param forecast_values_sql: forecast value objects
    :return: list of Forecast pydantic objects
    """

    # dictionary of forecasts by gsp_id
    forecasts_by_gsp = {}
    for forecast_value_sql in forecast_values_sql:
        gsp_id = forecast_value_sql.forecast.location.gsp_id

        forecast_value: ForecastValue = ForecastValue.model_validate(
            forecast_value_sql, from_attributes=True
        )

        if gsp_id in forecasts_by_gsp.keys():
            forecasts_by_gsp[gsp_id].forecast_values.append(forecast_value)
        else:
            forecast = ForecastSQL(
                model=forecast_value_sql.forecast.model,
                forecast_creation_time=forecast_value_sql.forecast.forecast_creation_time,
                location=forecast_value_sql.forecast.location,
                input_data_last_updated=forecast_value_sql.forecast.input_data_last_updated,
                forecast_values=[forecast_value_sql],
                historic=forecast_value_sql.forecast.historic,
            )
            forecast = Forecast.model_validate(forecast, from_attributes=True)
            forecasts_by_gsp[gsp_id] = forecast

    forecasts = [forecast for forecast in forecasts_by_gsp.values()]

    return forecasts


def convert_df_to_national_forecast(
    forecast_values_df: pd.DataFrame, session: Session, model_name: str, version: str
) -> ForecastSQL:
    """
    Make a ForecastSQL object from a dataframe.

    :param forecast_values_df: Dataframe containing
        -- target_datetime_utc
        -- forecast_mw
        -- (Optional) forecast_mw_plevel_10
        -- (Optional) forecast_mw_plevel_90
    :param: session: database session
    :param: model_name: the name of the model
    :param: version: the version of the model
    :return: forecast object
    """

    logger.debug("Converting dataframe to National Forecast")

    assert "target_datetime_utc" in forecast_values_df.columns
    assert "forecast_mw" in forecast_values_df.columns

    # get last input data
    input_data_last_updated = get_latest_input_data_last_updated(session=session)

    # get model name
    model = get_model(name=model_name, version=version, session=session)

    # get location
    location = get_location(session=session, gsp_id=0)

    # make forecast values
    forecast_values = []
    for i, forecast_value in forecast_values_df.iterrows():
        # add timezone
        target_time = forecast_value.target_datetime_utc.replace(tzinfo=timezone.utc)
        forecast_value_sql = ForecastValue(
            target_time=target_time,
            expected_power_generation_megawatts=forecast_value.forecast_mw,
        ).to_orm()
        forecast_value_sql.adjust_mw = 0.0

        forecast_value_sql.properties = {}
        if "forecast_mw_plevel_10" in forecast_values_df.columns:
            forecast_value_sql.properties["10"] = forecast_value.forecast_mw_plevel_10

        if "forecast_mw_plevel_90" in forecast_values_df.columns:
            forecast_value_sql.properties["90"] = forecast_value.forecast_mw_plevel_90

        forecast_values.append(forecast_value_sql)

    # make forecast object
    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=datetime.now(tz=timezone.utc),
        location=location,
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
        historic=False,
    )

    return forecast

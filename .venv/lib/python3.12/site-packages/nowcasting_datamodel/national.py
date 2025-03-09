""" Make national forecasts """

import logging
from typing import List

import pandas as pd
from sqlalchemy.orm.session import Session

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models import (
    Forecast,
    ForecastSQL,
    ForecastValue,
    MLModel,
    national_gb_label,
)
from nowcasting_datamodel.read.read import get_latest_input_data_last_updated, get_location

logger = logging.getLogger(__name__)


def make_national_forecast(
    forecasts: List[ForecastSQL], session: Session, n_gsps: int = N_GSP
) -> ForecastSQL:
    """This takes a list of forecast and adds up all the forecast values

    Note that a different method to do this, would be to do this in the database
    """
    assert (
        len(forecasts) == n_gsps
    ), f"The number of forecast was only {len(forecasts)}, it should be {n_gsps}"

    # check the gsp ids are unique
    gsps = [int(f.location.gsp_id) for f in forecasts]
    seen = set()
    dupes = [x for x in gsps if x in seen or seen.add(x)]
    assert len(seen) == n_gsps, f"Found non unique GSP ids {dupes}"

    location = get_location(gsp_id=0, session=session, label=national_gb_label)
    input_data_last_updated = get_latest_input_data_last_updated(session=session)

    # make pandas dataframe of all the forecast values with a gsp id
    forecast_values_flat = []
    for forecast in forecasts:
        gsp_id = forecast.location.gsp_id

        one_gsp = pd.DataFrame(
            [
                ForecastValue.model_validate(value, from_attributes=True).model_dump()
                for value in forecast.forecast_values
            ]
        )
        adjusts_mw = [f.adjust_mw for f in forecast.forecast_values]
        one_gsp["gps_id"] = gsp_id
        one_gsp["_adjust_mw"] = adjusts_mw
        # this is a data frame with the follow columns
        # - gsp_id
        # - target_time
        # - expected_power_generation_megawatts

        forecast_values_flat.append(one_gsp)

    forecast_values_df = pd.concat(forecast_values_flat)
    forecast_values_df["count"] = (
        1  # this will be used later to check that there are 338 forecasts for each target_time
    )

    # group by target time
    forecast_values_df = forecast_values_df.groupby(["target_time"]).sum()
    forecast_values_df.reset_index(inplace=True)
    if (forecast_values_df["count"] != n_gsps).all():
        m = f'The should should be {n_gsps}, but instead it is {forecast_values_df["count"]}'
        logger.debug(m)
        raise Exception(m)

    # change back to ForecastValue
    forecast_values = []
    for _, row in forecast_values_df.iterrows():
        f = ForecastValue(
            target_time=row["target_time"],
            expected_power_generation_megawatts=row["expected_power_generation_megawatts"],
        )
        f._adjust_mw = row["_adjust_mw"]
        forecast_values.append(f)

    # change to ForecastValueSQL
    forecast_values = [forecast_value.to_orm() for forecast_value in forecast_values]

    # change to sql
    model = forecasts[0].model
    if isinstance(model, MLModel):
        model = model.to_orm()

    national_forecast = ForecastSQL(
        forecast_values=forecast_values,
        location=location,
        model=model,
        input_data_last_updated=input_data_last_updated,
        forecast_creation_time=forecasts[0].forecast_creation_time,
        historic=False,
    )

    # validate
    _ = Forecast.model_validate(national_forecast, from_attributes=True)

    return national_forecast

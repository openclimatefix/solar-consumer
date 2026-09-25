"""Functions to save forecasts to the Data-platform

https://github.com/openclimatefix/data-platform

"""

import datetime
from importlib.metadata import version

import pandas as pd
from loguru import logger
from ocf import dp

from solar_consumer.save.data_platform.utils import _list_locations


async def save_forecasts_to_data_platform(
    data_df: pd.DataFrame, 
    client: dp.DataPlatformDataServiceStub,
    model_tag: str, 
    model_version: str, 
    init_time_utc: datetime,
    country: str = "gb",
) -> None:
    """
    Save NESO solar forecast data to the data platform.

    The NESO forecast is national-level (gsp_id=0) and contains 3 types of forecasts
    that get updated at different times.

    Args:
        data_df: DataFrame with columns 'target_datetime_utc' and 'solar_generation_kw'
        client: Data platform client stub
        model_tag: Model tag identifier for the forecast
        model_version: Model version string
        country: Country code for the forecast data.
    """
    # 1. Ensure the forecaster exists
    forecaster = await create_forecaster_if_not_exists(client, model_tag, model_version)
    logger.info(f"Using forecaster: {forecaster.forecaster_name} (version: {forecaster.forecaster_version})")

    # 2. Get the national location (gsp_id=0)
    locations_data = await _list_locations(
        client,
        location_type=[dp.LocationType.NATION],
        country=country,
    )
    
    # Find the national location
    national_location = None
    
    for loc in locations_data:
        metadata = loc.get("metadata", {})
        if country == "gb":
            # GB specific: look for gsp_id=0
            gsp_id = metadata.get("gsp_id", {}).get("number_value")
            if gsp_id == 0:
                national_location = loc
                break
        else:
            national_location = loc
            break
    
    if national_location is None:
        raise ValueError(
            f"No national location found in data platform for {country.upper()}. "
            "Please ensure it exists before saving forecasts."
        )
    
    location_uuid = national_location["location_uuid"]
    effective_capacity_watts = float(national_location.get("effective_capacity_watts", 0))
    
    if effective_capacity_watts <= 0:
        raise ValueError(
            f"National location (gsp_id=0) has invalid effective_capacity_watts: {effective_capacity_watts}. "
            "Cannot calculate forecast fractions."
        )

    # 3. Determine init_time_utc and horizon mins
    init_time_utc = init_time_utc.replace(tzinfo=None)
    # create horizon mins
    target_datetime_utc = pd.to_datetime(data_df['target_datetime_utc'].values)
    horizons_mins = (target_datetime_utc - init_time_utc).total_seconds() / 60
    horizons_mins = horizons_mins.astype(int)

    # Get p50 fractions
    p50s = data_df["solar_generation_kw"].values.astype(float)
    p50s = p50s * 1000 / float(effective_capacity_watts)  # kW -> W, then fraction
    
    forecast_values = []
    
    for h, p50 in zip(horizons_mins, p50s, strict=True):
        if h < 0:
            # Skip targets in the past relative to init time
            continue
        forecast_values.append(
            dp.CreateForecastRequestForecastValue(
                horizon_mins=h,
                p50_fraction=p50,
                other_statistics_fractions={},
            )
        )    
    
    if not forecast_values:
        logger.warning("No valid forecast values to save.")
        return
    
    # 4. Save forecast using the forecaster
    await client.create_forecast(
        dp.CreateForecastRequest(
            forecaster=forecaster,
            location_uuid=location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            init_time_utc=init_time_utc.replace(tzinfo=datetime.UTC),
            values=forecast_values,
        )
    )
    
    logger.info(
        f"Saved {len(forecast_values)} forecast values to data platform for "
        f"location {location_uuid} (init_time: {init_time_utc})",
    )

async def create_forecaster_if_not_exists(
    client: dp.DataPlatformDataServiceStub,
    model_tag: str,
    model_version: str | None = None,
) -> dp.Forecaster:
    """Create the current forecaster if it does not exist.
    
    Args:
        client: Data platform client stub
        model_tag: Model tag identifier (will be converted to forecaster name)
        model_version: Version string for the forecaster. If None, uses package version.
    
    Returns:
        The Forecaster object (existing or newly created/updated)
    """
    name = model_tag.replace("-", "_")
    
    if model_version is None:
        app_version = version("neso-solar-consumer")
    else:
        app_version = model_version

    list_forecasters_request = dp.ListForecastersRequest(
        forecaster_names_filter=[name],
    )
    list_forecasters_response = await client.list_forecasters(list_forecasters_request)

    if len(list_forecasters_response.forecasters) > 0:
        filtered_forecasters = [
            f for f in list_forecasters_response.forecasters if f.forecaster_version == app_version
        ]
        if len(filtered_forecasters) == 1:
            # Forecaster exists, return it
            return filtered_forecasters[0]
        else:
            # Forecaster version does not exist, update it
            update_forecaster_request = dp.UpdateForecasterRequest(
                name=name,
                new_version=app_version,
            )
            update_forecaster_response = await client.update_forecaster(update_forecaster_request)
            return update_forecaster_response.forecaster
    else:
        # Forecaster does not exist, create it
        create_forecaster_request = dp.CreateForecasterRequest(
            name=name,
            version=app_version,
        )
        create_forecaster_response = await client.create_forecaster(create_forecaster_request)
        return create_forecaster_response.forecaster

from loguru import logger
from datetime import datetime, timezone
import pandas as pd
from nowcasting_datamodel.models import ForecastSQL, ForecastValue
from nowcasting_datamodel.read.read import (
    get_latest_input_data_last_updated,
    get_location,
)
from nowcasting_datamodel.read.read_models import get_model


def format_to_forecast_sql(data: pd.DataFrame, model_tag: str, model_version: str, session) -> list:
    """
    Format solar forecast data into a ForecastSQL object.

    Parameters:
        data (pd.DataFrame): DataFrame containing `Datetime_GMT` (UTC) and `solar_forecast_kw`.
        model_tag (str): Model tag to fetch model metadata.
        model_version (str): Model version to fetch model metadata.
        session: Database session.

    Returns:
        list: A list containing a single ForecastSQL object.
    """
    logger.info("Starting format_to_forecast_sql process...")

    # Step 1: Retrieve model metadata
    model = get_model(name=model_tag, version=model_version, session=session)
    input_data_last_updated = get_latest_input_data_last_updated(session=session)

    # Step 2: Fetch or create the location
    location = get_location(session=session, gsp_id=0)  # National forecast

    # Step 3: Process all rows into ForecastValue objects
    forecast_values = []
    for _, row in data.iterrows():
        if pd.isnull(row["target_datetime_utc"]) or pd.isnull(row["solar_generation_kw"]):
            logger.warning(f"Skipping row due to missing data: {row}")
            continue

        target_time = row["target_datetime_utc"]

        # Create ForecastValue object
        forecast_value = ForecastValue(
            target_time=target_time,
            expected_power_generation_megawatts=row["solar_generation_kw"] / 1000,  # Convert to MW
        ).to_orm()
        forecast_values.append(forecast_value)

    # Step 4: Create a single ForecastSQL object
    forecast = ForecastSQL(
        model=model,
        forecast_creation_time=datetime.now(tz=timezone.utc),
        location=location,
        input_data_last_updated=input_data_last_updated,
        forecast_values=forecast_values,
        historic=False,
    )
    logger.info(f"Created ForecastSQL object with {len(forecast_values)} forecast values.")

    # Return a single ForecastSQL object in a list
    return [forecast]

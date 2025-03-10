import logging
import pandas as pd
from datetime import datetime, timezone
from nowcasting_datamodel.read.read import get_latest_input_data_last_updated, get_location
from nowcasting_datamodel.read.read_models import get_model

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def format_for_database(data: pd.DataFrame, model_tag: str, model_version: str, session) -> pd.DataFrame:
    """
    Format solar forecast data specifically for database storage.
    
    Parameters:
        data (pd.DataFrame): DataFrame containing `Datetime_GMT` (UTC) and `solar_forecast_kw`.
        model_tag (str): Model tag to fetch model metadata.
        model_version (str): Model version to fetch model metadata.
        session: Database session.
        
    Returns:
        pd.DataFrame: Formatted DataFrame ready for database storage.
    """
    logger.info("Formatting forecast data for database storage...")
    
    # Use existing format_forecast function
    return format_forecast(data, model_tag, model_version, session)

def format_forecast(data: pd.DataFrame, model_tag: str, model_version: str, session) -> pd.DataFrame:
    """
    Format solar forecast data into a standardized Pandas DataFrame.

    Parameters:
        data (pd.DataFrame): DataFrame containing `Datetime_GMT` (UTC) and `solar_forecast_kw`.
        model_tag (str): Model tag to fetch model metadata.
        model_version (str): Model version to fetch model metadata.
        session: Database session.

    Returns:
        pd.DataFrame: Formatted DataFrame with additional metadata.
    """
    logger.info("Starting forecast formatting process...")
    try:
        # Validate required columns in the input DataFrame
        _validate_columns(data)

        # Retrieve metadata from the database
        model = _get_model_metadata(model_tag, model_version, session)
        input_data_last_updated = get_latest_input_data_last_updated(session=session)
        location = _get_location_metadata(session)

        # Drop rows with missing values
        data = data.dropna(subset=["Datetime_GMT", "solar_forecast_kw"])

        # Ensure Datetime_GMT is in datetime format
        data["Datetime_GMT"] = pd.to_datetime(data["Datetime_GMT"], utc=True)

        # Convert power to MW and add as a new column
        data["solar_forecast_mw"] = data["solar_forecast_kw"] / 1000
        data.drop(columns=["solar_forecast_kw"], inplace=True)

        # Add metadata columns
        data["model_name"] = model.name
        data["model_version"] = model.version
        data["forecast_creation_time"] = datetime.now(tz=timezone.utc)
        data["location"] = _get_location_name(location)

        logger.info(f"Formatted forecast data with {len(data)} entries.")
        return data
    except ValueError as e:
        logger.error(f"Validation error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during formatting: {e}")
    return pd.DataFrame()

def _validate_columns(data: pd.DataFrame):
    """
    Validate that required columns exist in the input DataFrame.

    Parameters:
        data (pd.DataFrame): Input DataFrame.

    Raises:
        ValueError: If required columns are missing.
    """
    required_columns = {"Datetime_GMT", "solar_forecast_kw"}
    if not required_columns.issubset(data.columns):
        raise ValueError(f"Missing required columns: {required_columns - set(data.columns)}")

def _get_model_metadata(model_tag: str, model_version: str, session):
    """
    Retrieve model metadata from the database.

    Parameters:
        model_tag (str): Model tag to fetch metadata.
        model_version (str): Model version to fetch metadata.
        session: Database session.

    Returns:
        Model object containing metadata.
    """
    try:
        return get_model(name=model_tag, version=model_version, session=session)
    except Exception as e:
        logger.error(f"Error fetching model metadata: {e}")
        raise

def _get_location_metadata(session):
    """
    Retrieve location metadata from the database.

    Parameters:
        session: Database session.

    Returns:
        Location object containing metadata.
    """
    try:
        return get_location(session=session, gsp_id=0) # National forecast
    except Exception as e:
        logger.error(f"Error fetching location metadata: {e}")
        raise

def _get_location_name(location):
    """
    Safely retrieve the name attribute from a location object.

    Parameters:
        location: Location object.

    Returns:
        str: Location name or string representation if name attribute is missing.
    """
    try:
        return location.name
    except AttributeError:
        logger.warning("Location object does not have a 'name' attribute.")
        return str(location)

# Backward-compatible alias for the old function name
format_to_forecast_sql = format_forecast

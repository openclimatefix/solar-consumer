"""
Main Script to Fetch, Format, and Save NESO Solar Forecast Data

This script orchestrates the following steps:
1. Fetches solar forecast data using the `fetch_data` function.
2. Formats the forecast data into `ForecastSQL` objects using `format_forecast.py`.
3. Saves the formatted forecasts into the database using `save_forecast.py`.
"""

import os
import logging
from neso_solar_consumer.fetch_data import fetch_data
from neso_solar_consumer.format_forecast import format_to_forecast_sql
from neso_solar_consumer.save_forecast import (
    save_forecasts_to_csv,
    save_forecasts_to_db,
)
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models import Base_Forecast
from neso_solar_consumer import __version__  # Import version from __init__.py

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def app(db_url: str, save_method: str, csv_dir: str = None, country:str='uk'):
    """
    Main application function to fetch, format, and save solar forecast data.

    Parameters:
        db_url (str): Database connection URL from an environment variable.
        save_method (str): Method to save the forecast data. Options are "db" or "csv".
        csv_dir (str, optional): Directory to save CSV files if save_method is "csv".
        country (str): Country code for fetching data. Default is "uk".
    """
    logger.info(f"Starting the NESO Solar Forecast pipeline (version: {__version__}).")

    # Use the `Neso` class for hardcoded configuration
    model_tag = "neso-solar-forecast"

    # Initialize database connection
    connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=False)

    try:
        with connection.get_session() as session:
            # Step 1: Fetch forecast data (returns as pd.Dataframe)
            logger.info("Fetching forecast data.")
            forecast_data = fetch_data()

            if forecast_data.empty:
                logger.warning("No data fetched. Exiting the pipeline.")
                return

            # Step 2: Formate and save the forecast data
            # A. Format forecast to database object and save
            if save_method == "db":
                logger.info(f"Formatting {len(forecast_data)} rows of forecast data.")
                forecasts = format_to_forecast_sql(
                    data=forecast_data,
                    model_tag=model_tag,
                    model_version=__version__,  # Use the version from __init__.py
                    session=session,
                )

                if not forecasts:
                    logger.warning("No forecasts generated. Exiting the pipeline.")
                    return

                logger.info(f"Generated {len(forecasts)} ForecastSQL objects.")

                # Saving formatted forecasts to the database
                logger.info("Saving forecasts to the database.")
                save_forecasts_to_db(forecasts, session)

            # B. Save directly to CSV
            elif save_method == "csv":
                logger.info(
                    f"Saving {len(forecast_data)} rows of forecast data directly to CSV."
                )
                save_forecasts_to_csv(forecast_data, csv_dir=csv_dir)

            # C. TODO: Potential new save methods
            elif save_method == "site-db":
                logger.info("Saving forecasts to the site database.")
                #
                pass
            else:
                logger.error(f"Unsupported save method: {save_method}. Exiting.")
                return

            logger.info("Forecast pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in the forecast pipeline: {e}")
        raise


if __name__ == "__main__":
    # Step 1: Fetch the database URL from the environment variable
    db_url = os.getenv("DB_URL")  # Change from "DATABASE_URL" to "DB_URL"
    country = os.getenv("COUNTRY", "uk")

    save_method = os.getenv("SAVE_METHOD", "db").lower()  # Default to "db"
    csv_dir = os.getenv("CSV_DIR")

    if save_method == "csv" and not csv_dir:
        logger.error(
            "CSV_DIR environment variable is required for CSV saving. Exiting."
        )
        exit(1)

    if not db_url:
        logger.error("DB_URL environment variable is not set. Exiting.")
        exit(1)

    # Step 2: Run the application
    app(db_url=db_url, save_method=save_method, csv_dir=csv_dir,country=country)

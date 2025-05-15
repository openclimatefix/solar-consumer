"""
Main Script to Fetch, Format, and Save NESO Solar Forecast Data

This script orchestrates the following steps:
1. Fetches solar forecast data using the `fetch_data` function.
2. Formats the forecast data into `ForecastSQL` objects using `format_forecast.py`.
3. Saves the formatted forecasts into the database using `save_forecast.py`.
"""

import os
from loguru import logger
from solar_consumer.fetch_data import fetch_data
from solar_consumer.format_forecast import format_to_forecast_sql
from solar_consumer.save_forecast import (
    save_forecasts_to_csv,
    save_forecasts_to_db,
    save_generation_to_site_db,
    save_forecasts_to_site_db,
)
from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models import Base_Forecast
from solar_consumer import __version__


def app(
    db_url: str,
    save_method: str,
    csv_dir: str = None,
    country: str = "uk",
    historic_or_forecast: str = "generation",
):
    logger.info(f"🚀 Starting NESO Solar Forecast pipeline (v{__version__})")

    # Set model_tag based on country
    if country == "uk":
        model_tag = "neso-solar-forecast"
    elif country == "nl":
        model_tag = "ned-nl-national"
    elif country == "in":
        model_tag = "upsl-solar-generation"
    else:
        model_tag = f"{country}-solar-unknown"

    # Fetch the data
    logger.info(f"📡 Fetching {historic_or_forecast} data for {country}")
    forecast_data = fetch_data(country=country, historic_or_forecast=historic_or_forecast)

    try:
        if forecast_data.empty:
            logger.warning("⚠️ No data fetched. Exiting the pipeline.")
            return

        # ✅ Optional: Print India value clearly
        if country == "in":
            gen_mw = forecast_data.iloc[0]["solar_generation_kw"] / 1000
            logger.info(f"✅ UPSLDC Solar Generation: {gen_mw:.2f} MW at {forecast_data.iloc[0]['target_datetime_utc']}")

        # DB Save Path
        if save_method == "db":
            connection = DatabaseConnection(url=db_url, base=Base_Forecast, echo=False)
            with connection.get_session() as session:
                logger.info(f"🛠 Formatting {len(forecast_data)} rows")
                forecasts = format_to_forecast_sql(
                    data=forecast_data,
                    model_tag=model_tag,
                    model_version=__version__,
                    session=session,
                )
                if not forecasts:
                    logger.warning("⚠️ No forecasts generated. Exiting.")
                    return

                logger.info(f"📦 Generated {len(forecasts)} ForecastSQL objects")
                logger.info("💾 Saving forecasts to the database...")
                save_forecasts_to_db(forecasts, session)

        # CSV Save Path
        elif save_method == "csv":
            logger.info(f"💾 Saving {len(forecast_data)} rows to CSV")
            save_forecasts_to_csv(forecast_data, csv_dir=csv_dir)

        # Site-DB Save Path
        elif save_method == "site-db":
            connection = DatabaseConnection(url=db_url, echo=False)
            with connection.get_session() as session:
                if historic_or_forecast == "generation":
                    logger.info("💾 Saving generation data to site DB")
                    save_generation_to_site_db(
                        session=session,
                        generation_data=forecast_data,
                        country=country,
                    )
                elif historic_or_forecast == "forecast":
                    logger.info("💾 Saving forecast data to site DB")
                    save_forecasts_to_site_db(
                        session=session,
                        forecast_data=forecast_data,
                        country=country,
                        model_tag=model_tag,
                        model_version=__version__,
                    )
                else:
                    logger.warning("⚠️ Unsupported historic_or_forecast value for site-db")

        else:
            logger.error(f"❌ Unsupported save method: {save_method}. Exiting.")
            return

        logger.info("✅ Forecast pipeline completed successfully.")

    except Exception as e:
        logger.error(f"💥 Error in forecast pipeline: {e}")
        raise


if __name__ == "__main__":
    db_url = os.getenv("DB_URL")
    country = os.getenv("COUNTRY", "uk")
    save_method = os.getenv("SAVE_METHOD", "db").lower()
    csv_dir = os.getenv("CSV_DIR")
    historic_or_forecast = os.getenv("HISTORIC_OR_FORECAST", "generation").lower()

    if save_method == "csv" and not csv_dir:
        logger.error("❌ CSV_DIR is required for CSV saving. Exiting.")
        exit(1)
    if save_method in ["db", "site-db"] and not db_url:
        logger.error("❌ DB_URL is required for database saving. Exiting.")
        exit(1)

    app(
        db_url=db_url,
        save_method=save_method,
        csv_dir=csv_dir,
        country=country,
        historic_or_forecast=historic_or_forecast,
    )

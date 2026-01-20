import os
from loguru import logger
import pandas as pd


def save_forecasts_to_csv(forecasts: pd.DataFrame, csv_dir: str):
    """Save forecasts to a CSV file.

    Parameters:
        forecasts (pd.DataFrame): DataFrame containing forecast data to save.
        csv_dir (str): Directory to save CSV files.

    Return:
        None
    """
    # Check if forecasts is empty
    if forecasts.empty:
        logger.warning("No forecasts provided to save!")
        return

    if not csv_dir:  # check if directory csv directory provided
        raise ValueError("CSV directory is not provided for CSV saving.")

    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, "forecast_data.csv")

    try:
        forecasts.drop(
            columns=["_sa_instance_state"], errors="ignore", inplace=True
        )  # Remove SQLAlchemy metadata

        logger.info(f"Saving forecasts to CSV at {csv_path}")
        forecasts.to_csv(csv_path, index=False)
        logger.info(f"Successfully saved {len(forecasts)} forecasts to CSV.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts to CSV: {e}")
        raise e
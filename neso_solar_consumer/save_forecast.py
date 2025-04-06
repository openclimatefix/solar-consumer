import logging
from nowcasting_datamodel.save.save import save
from sqlalchemy.orm.session import Session
import os
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def save_forecasts(
    forecasts: list | pd.DataFrame,
    session: Session,
    save_method: str = "db",
    csv_dir: str = None,
):
    """
    Save forecasts either to the database or as a CSV file.

    Parameters:
        forecasts (list[ForecastSQL] | pd.DataFrame): Forecast data to be saved; can be a list of ForecastSQL objects or a DataFrame.
        session (Session): SQLAlchemy session for database access.
        save_method (str): "db" to save in database, "csv" to save as CSV.
        csv_dir (str, optional): Directory to save CSV files if `save_method` is "csv".
    """
    # Check if forecasts is empty for both list and dataframe
    is_empty = False
    if isinstance(forecasts, pd.DataFrame):
        is_empty = forecasts.empty
    elif isinstance(forecasts, list):
        is_empty = len(forecasts) == 0
    else:
        raise ValueError("Invalid type for forecasts. Expected a list or DataFrame.")

    if is_empty:
        logger.warning("No forecasts provided to save!")
        return

    # save forecasts to database
    if save_method == "db":
        try:
            logger.info("Saving forecasts to the database.")
            save(
                forecasts=forecasts,
                session=session,
            )
            logger.info(
                f"Successfully saved {len(forecasts)} forecasts to the database."
            )
        except Exception as e:
            logger.error(f"An error occurred while saving forecasts: {e}")
            raise e

    # save dataframe to csv
    elif save_method == "csv":
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

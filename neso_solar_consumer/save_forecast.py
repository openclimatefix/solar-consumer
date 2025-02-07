import logging
from nowcasting_datamodel.save.save import save
from sqlalchemy.orm.session import Session
from nowcasting_datamodel.models import ForecastSQL
import os
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def save_forecasts(forecasts: list, session: Session,  save_method: str = "db", csv_dir: str = None):
    """
    Save forecasts either to the database or as a CSV file.

    Parameters:
        forecasts (list): The list of ForecastSQL objects to save.
        session (Session): SQLAlchemy session for database access.
        save_method (str): "db" to save in database, "csv" to save as CSV.
        csv_dir (str, optional): Directory to save CSV files if `save_method` is "csv".
    """
    if not forecasts:
        logger.warning("No forecasts provided to save!")
        return

    if save_method == "db":
        try:
            logger.info("Saving forecasts to the database.")
            save(
                forecasts=forecasts,
                session=session,
            )
            logger.info(f"Successfully saved {len(forecasts)} forecasts to the database.")
        except Exception as e:
            logger.error(f"An error occurred while saving forecasts: {e}")
            raise e
        
    elif save_method == "csv":
        if not csv_dir: #check if directory csv directory provided
            raise ValueError("CSV directory is not provided for CSV saving.")
        
        os.makedirs(csv_dir, exist_ok=True)
        csv_path = os.path.join(csv_dir, "forecast_data.csv")

        try:
            # Convert ForecastSQL objects to DataFrame
            df = pd.DataFrame([forecast.__dict__ for forecast in forecasts])
            df.drop(columns=["_sa_instance_state"], errors="ignore", inplace=True)  # Remove SQLAlchemy metadata

            logger.info(f"Saving forecasts to CSV at {csv_path}")
            df.to_csv(csv_path, index=False)
            logger.info(f"Successfully saved {len(forecasts)} forecasts to CSV.")
        except Exception as e:
            logger.error(f"An error occurred while saving forecasts to CSV: {e}")
            raise e
    else:
        raise ValueError(f"Unsupported save method: {save_method}")

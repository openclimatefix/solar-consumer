import logging
import os
import pandas as pd
from sqlalchemy.orm.session import Session
from nowcasting_datamodel.save.save import save
from nowcasting_datamodel.models import ForecastSQL

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def save_forecasts(df: pd.DataFrame, session: Session = None, save_method: str = "db", csv_dir: str = None):
    """
    Save forecasts either to the database or as a CSV file.

    Parameters:
        df (pd.DataFrame): DataFrame containing forecast data.
        session (Session, optional): SQLAlchemy session for database access.
        save_method (str): "db" to save in database, "csv" to save as CSV.
        csv_dir (str, optional): Directory to save CSV files if `save_method` is "csv".
    """
    if df.empty:
        logger.warning("No forecasts provided to save!")
        return

    if save_method == "db":
        if session is None:
            raise ValueError("Database session is required for saving to DB.")
        save_to_db(df, session)
    elif save_method == "csv":
        if not csv_dir:
            raise ValueError("CSV directory is required for saving to CSV.")
        save_to_csv(df, csv_dir)
    else:
        raise ValueError(f"Unsupported save method: {save_method}")

def save_to_db(df: pd.DataFrame, session: Session):
    """Convert DataFrame to ForecastSQL objects and save to DB."""
    try:
        forecasts = [ForecastSQL(**row) for row in df.to_dict(orient="records")]
        logger.info("Saving forecasts to the database.")
        save(forecasts=forecasts, session=session)
        logger.info(f"Successfully saved {len(forecasts)} forecasts to the database.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts to DB: {e}")
        raise e

def save_to_csv(df: pd.DataFrame, csv_dir: str):
    """Save forecasts DataFrame to CSV."""
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, "forecast_data.csv")

    try:
        logger.info(f"Saving forecasts to CSV at {csv_path}")
        df.to_csv(csv_path, index=False)
        logger.info(f"Successfully saved {len(df)} forecasts to CSV.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts to CSV: {e}")
        raise e

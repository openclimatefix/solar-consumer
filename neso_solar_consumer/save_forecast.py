import logging
import os
import pandas as pd
from sqlalchemy.orm.session import Session
from nowcasting_datamodel.save.save import save
from nowcasting_datamodel.models import ForecastSQL

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def save_to_database(df: pd.DataFrame, session: Session):
    """
    Format forecast to database object and save.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing forecast data.
        session (Session): SQLAlchemy session for database access.
    """
    _validate_session(session)
    save_to_db(df, session)

def save_to_csv_file(df: pd.DataFrame, csv_dir: str):
    """
    Save forecast directly to CSV file.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing forecast data.
        csv_dir (str): Directory to save CSV files.
    """
    _validate_csv_dir(csv_dir)
    save_to_csv(df, csv_dir)

# Placeholder for future methods
def save_with_new_method(df: pd.DataFrame, **kwargs):
    """
    New method to save the forecast data (placeholder for future extension).
    
    Parameters:
        df (pd.DataFrame): DataFrame containing forecast data.
        **kwargs: Additional parameters specific to this saving method.
    """
    logger.info("New saving method called (not yet implemented)")
    # Implementation to be added later

# For backward compatibility
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

    try:
        if save_method == "db":
            save_to_database(df, session)
        elif save_method == "csv":
            save_to_csv_file(df, csv_dir)
        else:
            raise ValueError(f"Unsupported save method: {save_method}")
    except ValueError as e:
        logger.error(f"Validation error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while saving forecasts: {e}")

def save_to_db(df: pd.DataFrame, session: Session):
    """
    Convert DataFrame to ForecastSQL objects and save to DB.

    Parameters:
        df (pd.DataFrame): DataFrame containing forecast data.
        session (Session): SQLAlchemy session for database access.
    """
    try:
        forecasts = [ForecastSQL(**row) for row in df.to_dict(orient="records")]
        logger.info("Saving forecasts to the database...")
        # Save forecasts using the provided session
        save(forecasts=forecasts, session=session)
        logger.info(f"Successfully saved {len(forecasts)} forecasts to the database.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts to DB: {e}")
        raise

def save_to_csv(df: pd.DataFrame, csv_dir: str):
    """
    Save forecasts DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): DataFrame containing forecast data.
        csv_dir (str): Directory to save the CSV file.
    """
    try:
        # Ensure directory exists
        _create_directory(csv_dir)
        # Define CSV file path
        csv_path = os.path.join(csv_dir, "forecast_data.csv")
        logger.info(f"Saving forecasts to CSV at {csv_path}...")
        # Save DataFrame to CSV
        df.to_csv(csv_path, index=False)
        logger.info(f"Successfully saved {len(df)} forecasts to CSV.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts to CSV: {e}")
        raise

def _validate_session(session):
    """
    Validate that a database session is provided.

    Parameters:
        session (Session): SQLAlchemy session for database access.

    Raises:
        ValueError: If session is None.
    """
    if session is None:
        raise ValueError("Database session is required for saving to DB.")

def _validate_csv_dir(csv_dir):
    """
    Validate that a directory is provided for saving CSV files.

    Parameters:
        csv_dir (str): Directory path.

    Raises:
        ValueError: If csv_dir is None or empty.
    """
    if not csv_dir:
        raise ValueError("CSV directory is required for saving to CSV.")

def _create_directory(directory):
    """
    Create a directory if it doesn't already exist.

    Parameters:
        directory (str): Directory path.
    """
    try:
        os.makedirs(directory, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to create directory {directory}: {e}")

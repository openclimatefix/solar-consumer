from loguru import logger
from nowcasting_datamodel.save.save import save


def save_forecasts_to_db(forecasts: list, session):
    """Save forecasts to the database.

    Parameters:
        forecasts (list): List of forecast objects to save.
        session: SQLAlchemy session for database access.

    Return:
        None
    """
    # Check if forecasts is empty
    if not forecasts:
        logger.warning("No forecasts provided to save!")
        return

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
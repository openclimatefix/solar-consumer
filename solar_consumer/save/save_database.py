from loguru import logger


def save_forecasts_to_db(forecasts: list, session):
    """Legacy function removed.

    The project no longer uses `nowcasting_datamodel` for saving forecasts. This
    function is kept for compatibility but will raise an error to indicate that
    the legacy DB save path has been removed.
    """
    logger.error("Legacy DB save method removed. Cannot save forecasts using this function.")
    raise RuntimeError("Legacy DB save method removed. Use 'site-db' or 'data-platform'.")
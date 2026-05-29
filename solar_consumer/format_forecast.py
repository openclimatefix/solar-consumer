from loguru import logger
from datetime import datetime, timezone
import pandas as pd
def format_to_forecast_sql(data: pd.DataFrame, model_tag: str, model_version: str, session) -> list:
    """Placeholder: legacy formatting removed.

    The project no longer uses `nowcasting_datamodel` types. This function is kept
    as a placeholder and will raise to indicate the legacy formatting path is
    no longer supported.
    """
    logger.error("Legacy format_to_forecast_sql removed (nowcasting_datamodel).")
    raise RuntimeError("Legacy format_to_forecast_sql removed. Use data-platform or site-db flows.")

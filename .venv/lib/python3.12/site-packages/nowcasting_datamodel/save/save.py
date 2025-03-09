""" Save forecasts to the database """

import logging
import os
from typing import List, Optional

from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models import PVSystem, PVSystemSQL
from nowcasting_datamodel.models.forecast import ForecastSQL
from nowcasting_datamodel.save.adjust import add_adjust_to_forecasts
from nowcasting_datamodel.save.update import (
    add_forecast_last_7_days_and_remove_old_data,
    change_forecast_value_to_forecast_last_7_days,
    update_all_forecast_latest,
)

logger = logging.getLogger(__name__)


def save(
    forecasts: List[ForecastSQL],
    session: Session,
    update_national: Optional[bool] = True,
    update_gsp: Optional[bool] = True,
    apply_adjuster: Optional[bool] = True,
    save_to_last_seven_days: Optional[bool] = True,
    remove_non_distinct_last_seven_days: bool = False,
):
    """
    Save forecast to database

    1. Add sqlalchemy onjects to database
    2. Saves to 'latest' table aswell

    Note that apply_adjuster=True can be overwritten by "USE_ADJUSTER" env var

    :param forecasts: list of sql forecasts
    :param session: database session
    :param update_national: Optional (default true), to update the national forecast
    :param update_gsp: Optional (default true), to update all the GSP forecasts
    :param apply_adjuster: Optional (default true), to apply the adjuster
    :param save_to_last_seven_days: Optional (default true), to save to the last seven days table
    :param remove_non_distinct_last_seven_days: Optional (default False), to only keep distinct
        forecast values in the forecast_value_last_seven_days table
        Please note that this does remove some data, which might be needed when calculating
        metrics. Another solution to this is to make a forecast <--> forecast_value a
        many-to-many relationship. This means one forecast will still have the full
        range of forecast values assign with it.
    """

    use_adjuster_env_var = bool(os.getenv("USE_ADJUSTER", "True").lower() in ["true", "1"])
    if apply_adjuster & (not use_adjuster_env_var):
        logger.warning(
            "USE_ADJUSTER is set to False, but apply_adjuster is set to True. "
            "Therefore the adjuster will not be applied to the forecasts."
        )
        apply_adjuster = False

    if apply_adjuster:
        logger.debug("Add Adjust to forecasts")
        add_adjust_to_forecasts(session=session, forecasts_sql=forecasts)

    # save objects to database
    logger.debug("Saving models")
    session.add_all(forecasts)
    session.commit()

    logger.debug("Updating to latest")
    update_all_forecast_latest(
        session=session, forecasts=forecasts, update_national=update_national, update_gsp=update_gsp
    )
    session.commit()

    if save_to_last_seven_days:
        logger.debug("Saving to last seven days table")
        save_all_forecast_values_seven_days(
            session=session,
            forecasts=forecasts,
            remove_non_distinct=remove_non_distinct_last_seven_days,
        )
        session.commit()


def save_pv_system(session: Session, pv_system: PVSystem) -> PVSystemSQL:
    """
    Get model object from name and version

    :param session: database session
    :param pv_system: pv system sql object

    return: PVSystem object

    """

    # start main query
    query = session.query(PVSystemSQL)

    # filter on pv_system_id and provider
    query = query.filter(PVSystemSQL.pv_system_id == pv_system.pv_system_id)
    query = query.filter(PVSystemSQL.provider == pv_system.provider)

    # get all results
    pv_systems = query.all()

    if len(pv_systems) == 0:
        logger.debug(
            f"Model for provider {pv_system.provider} and pv_system_id {pv_system.pv_system_id }"
            f"does not exist so going to add it"
        )

        session.add(pv_system.to_orm())
        session.commit()

    else:
        logger.debug(
            f"Model for provider {pv_system.provider} and pv_system_id {pv_system.pv_system_id} "
            f"is already there so not going to add it"
        )
        pv_system = pv_systems[0]

    return pv_system


def save_all_forecast_values_seven_days(
    session: Session, forecasts: List[ForecastSQL], remove_non_distinct: bool = False
):
    """
    Save all the forecast values in the last seven days table

    :param session: database sessions
    :param forecasts: list of forecasts
    :param remove_non_distinct: Optional (default False), to only keep distinct forecast values
        If the last saved forecast value is the same as the current one, it will not be saved
    """

    # get all values together
    forecast_values_last_7_days = []
    for forecast in forecasts:
        for forecast_value in forecast.forecast_values:
            forecast_value_last_7_days = change_forecast_value_to_forecast_last_7_days(
                forecast_value
            )
            forecast_values_last_7_days.append(forecast_value_last_7_days)

    # add them to the database
    add_forecast_last_7_days_and_remove_old_data(
        session=session,
        forecast_values=forecast_values_last_7_days,
        remove_non_distinct=remove_non_distinct,
    )

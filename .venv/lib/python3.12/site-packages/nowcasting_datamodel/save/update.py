""" Method to update latest forecast values """

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from sqlalchemy import delete, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.session import Session

from nowcasting_datamodel import N_GSP
from nowcasting_datamodel.models import ForecastValueSevenDaysSQL, LocationSQL, MLModelSQL
from nowcasting_datamodel.models.forecast import (
    ForecastSQL,
    ForecastValueLatestSQL,
    ForecastValueSQL,
)
from nowcasting_datamodel.read.read import (
    get_latest_forecast,
    get_latest_forecast_for_gsps,
)

logger = logging.getLogger(__name__)


def get_historic_forecast(
    session: Session, forecast: ForecastSQL, model_name: Optional[str] = None
) -> ForecastSQL:
    """
    Get historic forecast

    :param session:
    :param gsp_id:
    :param model_name: the model name to filter on
    :return:
    """

    gsp_id = forecast.location.gsp_id

    forecast_historic = get_latest_forecast(
        session=session, gsp_id=gsp_id, historic=True, model_name=model_name
    )

    if forecast_historic is None:
        logger.debug("Could not find a historic forecast, so will make one")

        forecast_historic = ForecastSQL(
            historic=True,
            forecast_creation_time=datetime.now(timezone.utc),
            location=forecast.location,
            input_data_last_updated=forecast.input_data_last_updated,
            model=forecast.model,
        )
        session.add(forecast_historic)
        session.commit()
    else:
        logger.debug("Found historic forecast")

    return forecast_historic


def upsert(session: Session, model, rows: List[dict]):
    """

    Upsert rows into model

    Insert or Update rows into model.
    This functions checks the primary keys, and if duplicate updates them.

    :param session: sqlalchemy Session
    :param model: the model
    :param rows: the rows we are going to update
    """
    table = model.__table__
    stmt = insert(table)
    primary_keys = [key.name for key in inspect(table).primary_key]
    update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}

    if not update_dict:
        raise ValueError("insert_or_update resulted in an empty update_dict")
    stmt = stmt.on_conflict_do_update(index_elements=primary_keys, set_=update_dict)
    session.execute(stmt, rows)
    session.commit()


def update_forecast_latest(
    forecast: ForecastSQL,
    session: Session,
    forecast_historic: Optional[ForecastSQL] = None,
    model_name: Optional[str] = None,
):
    """
    Update the forecast_values table

    1. Get the Forecast object that is for the latest,
        and don't load children
    2. create ForecastValueLatestSQL objects
    3. upsert them (update and or insert)

    :param forecast:
    :param model_name: the model name to filter on
    :return:
    """

    # 1. get forecast object
    if forecast_historic is None:
        forecast_historic = get_historic_forecast(
            session=session, forecast=forecast, model_name=model_name
        )

    # 2. create forecast value latest
    forecast_values = []
    for forecast_value in forecast.forecast_values:
        forecast_value_latest = change_forecast_value_to_latest(
            forecast_value,
            gsp_id=forecast.location.gsp_id,
            forecast_id=forecast_historic.id,
            model_id=forecast.model_id,
        )
        forecast_values.append(forecast_value_latest.__dict__)

    # update input_data_last_updated
    forecast_historic.input_data_last_updated_id = forecast.input_data_last_updated_id

    # upsert forecast values
    upsert(session=session, model=ForecastValueLatestSQL, rows=forecast_values)

    # update forecast creation time
    forecast_historic.forecast_creation_time = datetime.now(tz=timezone.utc)
    session.commit()


def change_forecast_value_to_latest(
    forecast_value: ForecastValueSQL,
    gsp_id: int,
    forecast_id: Optional[int] = None,
    model_id: Optional[int] = None,
) -> ForecastValueLatestSQL:
    """
    Make a ForecastValueLatestSQL from a ForecastValueQL object

    :param forecast_value: forecast value object
    :param gsp_id: gsp id
    :param forecast_id: forecast joining id
    :param model_id: model joining id
    :return: forecast value latest object
    """

    forecast_value_dict = {}
    for v in ["target_time", "expected_power_generation_megawatts", "adjust_mw", "properties"]:
        forecast_value_dict[v] = getattr(forecast_value, v)

    forecast_value_dict["gsp_id"] = gsp_id
    if forecast_id is not None:
        forecast_value_dict["forecast_id"] = forecast_id

    if model_id is not None:
        forecast_value_dict["model_id"] = model_id

    return ForecastValueLatestSQL(**forecast_value_dict)


def update_all_forecast_latest(
    forecasts: List[ForecastSQL],
    session: Session,
    update_national: Optional[bool] = True,
    update_gsp: Optional[bool] = True,
    filter_datetime: Optional[datetime] = None,
):
    """
    Update all latest forecasts

    :param forecasts: The forecasts that should be updated that should be used to update
        the latest table
    :param session: sqlalmacy session
    :param update_national: Optional (default true), to update the national forecast
    :param update_gsp: Optional (default true), to update all the GSP forecasts
    :param filter_datetime: Optional (default None), to remove all forecasts before this.
        Default is now minus 3 days
    """

    if filter_datetime is None:
        filter_datetime = datetime.now(timezone.utc) - timedelta(days=3)

    logger.debug("Getting the earliest forecast target time for the first forecast")
    forecast_values_target_times = [f.target_time for f in forecasts[0].forecast_values]
    model_name = forecasts[0].model.name
    model_version = forecasts[0].model.version
    start_target_time = min(forecast_values_target_times) - timedelta(days=1)
    logger.debug(
        f"First forecast start target time is {min(forecast_values_target_times)} "
        f"so will filter results on {start_target_time} for {model_name}"
    )

    gsp_ids = get_gsp_ids(include_national=update_national, include_gsps=update_gsp)
    if len(gsp_ids) == 0:
        logger.warning(f"Will not be updating any GSPs as {update_national=} and {update_gsp=}")
        return
    logger.debug(f"Will be update GSPs from {min(gsp_ids)} to {max(gsp_ids)}")

    # get all latest forecasts
    logger.debug("Getting all latest forecasts")
    forecasts_historic_all_gsps = get_latest_forecast_for_gsps(
        session=session,
        historic=True,
        preload_children=True,
        gsp_ids=gsp_ids,
        start_target_time=start_target_time,
        model_name=model_name,
    )
    # get all these ids, so we only have to load it once
    historic_gsp_ids = [forecast.location.gsp_id for forecast in forecasts_historic_all_gsps]
    logger.debug(f"Found {len(forecasts_historic_all_gsps)} historic forecasts")

    logger.debug(f"There are {len(forecasts)} forecasts that we will update")

    for forecast in forecasts:
        # chose the correct forecast historic
        logger.debug("Getting gsp")
        gsp_id = forecast.location.gsp_id
        if gsp_id not in gsp_ids:
            logger.debug(
                f"GSP id {gsp_id} will not be updated, "
                f"this is due to the options {update_national=} and {update_gsp=}"
            )
            continue

        logger.debug(f"Updating forecast for gsp_id {gsp_id}")

        forecast_historic = [
            forecast_historic_one_gsps
            for forecast_historic_one_gsps, historic_gsp_id in zip(
                forecasts_historic_all_gsps, historic_gsp_ids
            )
            if historic_gsp_id == gsp_id
        ]
        if len(forecast_historic) == 0:
            forecast_historic = None
            logger.debug(f"Could not find historic, so will be creating one (GSP id{gsp_id})")
        else:
            forecast_historic = forecast_historic[0]

            # update to latest model version
            forecast_historic.model.version = model_version

            logger.debug(f"Found historic for GSP id {gsp_id}")

        update_forecast_latest(
            forecast=forecast,
            session=session,
            forecast_historic=forecast_historic,
            model_name=forecast.model.name,
        )
        session.commit()

    # Delete forecasts older than 3 days from the forecast_latest table
    stmt = delete(ForecastValueLatestSQL).where(
        ForecastValueLatestSQL.target_time < filter_datetime
    )
    session.execute(stmt)
    session.commit()


def get_gsp_ids(include_national: bool = True, include_gsps: bool = True) -> List[int]:
    """
    Get list of gsps ids

    :param include_national: option to incldue gsp_id of 0
    :param include_gsps: option to include gsp_ids 1 to N_GSPS
    :return: list of gsps
    """

    gsp_ids = []
    if include_national:
        gsp_ids.append(0)
    if include_gsps:
        gsp_ids = gsp_ids + list(range(1, N_GSP + 1))
    return gsp_ids


def change_forecast_value_to_forecast_last_7_days(
    forecast: ForecastValueSQL,
) -> ForecastValueSevenDaysSQL:
    """
    Make a ForecastValueSevenDaysSQL object from a ForecastValueSQL

    :param forecast: original forecast value
    :return: forecast value seven days sql
    """

    forecast_new = ForecastValueSevenDaysSQL()
    for key in ForecastValueSQL.__table__.columns.keys():
        if hasattr(forecast, key):
            setattr(forecast_new, key, getattr(forecast, key))

    return forecast_new


def remove_non_distinct_forecast_values(
    session: Session, start_datetime: datetime, end_datetime: datetime
):
    """
    Remove any non-distinct forecast values

    We remove any non-distinct values from ForecastValueSevenDaysSQL. The values are distinct on
    - target_time
    - location_id
    - model_name
    - expected_power_generation_megawatts

    We keep the first value, and remove and values from later on.

    :param session: database session
    :param start_datetime: start datetime
    :param end_datetime: end_dateimte
    """

    logger.debug(f"Removing non distinct forecast values for {start_datetime} to {end_datetime}")

    # 1. sub query, these are the values we want to keep
    sub_query = session.query(ForecastValueSevenDaysSQL.uuid)

    # distinct
    sub_query = sub_query.distinct(
        ForecastValueSevenDaysSQL.target_time,
        ForecastSQL.location_id,
        MLModelSQL.name,
        ForecastValueSevenDaysSQL.expected_power_generation_megawatts,
    )

    # join
    sub_query = sub_query.join(ForecastSQL)
    sub_query = sub_query.join(LocationSQL)
    sub_query = sub_query.join(MLModelSQL)

    # filter on time
    sub_query = sub_query.filter(ForecastValueSevenDaysSQL.target_time >= start_datetime)
    sub_query = sub_query.filter(ForecastValueSevenDaysSQL.target_time < end_datetime)

    # order by
    sub_query = sub_query.order_by(
        ForecastValueSevenDaysSQL.target_time,
        ForecastSQL.location_id,
        MLModelSQL.name,
        ForecastValueSevenDaysSQL.expected_power_generation_megawatts,
        ForecastValueSevenDaysSQL.created_utc,  # get the first one
    )

    sub_query = sub_query.subquery()

    # 2. main query
    query = session.query(ForecastValueSevenDaysSQL)

    # filter on tiem
    query = query.filter(ForecastValueSevenDaysSQL.target_time >= start_datetime)
    query = query.filter(ForecastValueSevenDaysSQL.target_time < end_datetime)

    # select uuid not in subquery
    query = query.filter(ForecastValueSevenDaysSQL.uuid.notin_(sub_query))

    # delete all results
    query.delete()


def add_forecast_last_7_days_and_remove_old_data(
    forecast_values: List[ForecastValueSevenDaysSQL],
    session: Session,
    remove_non_distinct: bool = False,
):
    """
    Add forecast values and delete old values

    :param forecast_values:
    :param session:
    :param remove_non_distinct: Optional (default False), to only keep distinct forecast values
        If the last saved forecast value is the same as the current one, it will not be saved
    :return:
    """

    # add forecast
    session.add_all(forecast_values)

    # remove old data
    now_minus_7_days = datetime.now(tz=timezone.utc) - timedelta(days=7)
    now_minus_7_days = now_minus_7_days.replace(minute=0, second=0, microsecond=0)

    # remove any duplicate forecast values
    if remove_non_distinct:
        for i in range(0, 24 * 10):
            start_datetime = now_minus_7_days + timedelta(hours=i)
            end_datetime = start_datetime + timedelta(hours=1)
            remove_non_distinct_forecast_values(session, start_datetime, end_datetime)

    logger.debug(f"Removing data before {now_minus_7_days}")
    query = session.query(ForecastValueSevenDaysSQL)
    query = query.where(ForecastValueSevenDaysSQL.target_time < now_minus_7_days)
    query.delete()

    session.commit()

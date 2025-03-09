""" Read pv functions """

import logging
from datetime import datetime, timezone
from typing import List, Optional, Union

import pandas as pd
from sqlalchemy import desc, func
from sqlalchemy.orm import Session, contains_eager, joinedload

from nowcasting_datamodel.models import GSPYield, GSPYieldSQL, LocationSQL

logger = logging.getLogger(__name__)


def get_latest_gsp_yield(
    session: Session,
    gsps: Union[List[LocationSQL], List[int]],
    append_to_gsps: bool = False,
    regime: str = "in-day",
    datetime_utc: Optional[datetime] = None,
) -> Union[List[GSPYieldSQL], List[LocationSQL]]:
    """
    Get the last gsp yield data

    :param session: database sessions
    :param gsps: list of gsps
    :param append_to_gsps: append gsp yield to pv systems, or return pv systems.
        If appended the yield is access by 'pv_system.last_gsp_yield'
    :param regime: What regime the data is in, either 'in-day' or 'day-after'
    :param datetime_utc: Optional to filter on datetime
    :return: either list of gsp yields, or pv systems
    """

    if isinstance(gsps[0], int):
        gsp_ids = gsps
    else:
        gsp_ids = [gsp.gsp_id for gsp in gsps]

    # start main query
    query = session.query(GSPYieldSQL)
    query = query.join(LocationSQL)
    query = query.where(
        LocationSQL.id == GSPYieldSQL.location_id,
    )

    # filter on regime
    query = query.where(GSPYieldSQL.regime == regime)

    # only select on results per pv system
    query = query.distinct(LocationSQL.gsp_id)

    # select only th epv systems we want
    query = query.where(LocationSQL.gsp_id.in_(gsp_ids))

    if datetime_utc is not None:
        # filter on datetime
        query = query.where(GSPYieldSQL.datetime_utc >= datetime_utc)

    # order by 'created_utc' desc, so we get the latest one
    query = query.order_by(
        LocationSQL.gsp_id,
        desc(GSPYieldSQL.datetime_utc),
        desc(GSPYieldSQL.created_utc),
    )

    # get all results
    gsp_yields: List[GSPYieldSQL] = query.all()

    logger.debug(f"Found {len(gsp_yields)}  latest gsp yields")

    # add utc timezone
    for gsp_yield in gsp_yields:
        gsp_yield.datetime_utc = gsp_yield.datetime_utc.replace(tzinfo=timezone.utc)

    if not append_to_gsps:
        return gsp_yields
    else:
        # get list of gsp with last pv yields
        gsp_systems_with_gsp_yields = []
        for gsp_yield in gsp_yields:
            gsp = gsp_yield.location
            gsp.last_gsp_yield = gsp_yield

            gsp_systems_with_gsp_yields.append(gsp)

        logger.debug(f"Found {len(gsp_systems_with_gsp_yields)} gsps with yields")

        # add pv systems that dont have any pv yields
        gsp_ids_with_gsp_yields = [gsp.gsp_id for gsp in gsp_systems_with_gsp_yields]

        gsp_systems_with_no_gsp_yields = []
        for gsp in gsps:
            if gsp.gsp_id not in gsp_ids_with_gsp_yields:
                gsp.last_gsp_yield = None

                gsp_systems_with_no_gsp_yields.append(gsp)

        logger.debug(f"Found {len(gsp_systems_with_no_gsp_yields)} gsps with no yields")

        all_gsp_systems = gsp_systems_with_gsp_yields + gsp_systems_with_no_gsp_yields

        return all_gsp_systems


def get_gsp_yield(
    session: Session,
    gsp_ids: List[int],
    start_datetime_utc: datetime,
    regime: Optional[str] = None,
    end_datetime_utc: Optional[datetime] = None,
    filter_nans: Optional[bool] = True,
) -> List[GSPYieldSQL]:
    """
    Get the gsp yield values.

    :param session: sqlalmcy sessions
    :param gsp_ids: list of gsp ids that we filter on
    :param start_datetime_utc: filter values on this start datetime
    :param regime: filter query on this regim. Can be "in-day" or "day-after"
    :param end_datetime_utc: optional end datetime filter
    :param filter_nans: optional filter out nans. Default is True
    :return: list of gsp yields
    """

    # start main query
    query = session.query(GSPYieldSQL)
    query = query.join(LocationSQL)
    query = query.options(joinedload(GSPYieldSQL.location))
    query = query.where(
        LocationSQL.id == GSPYieldSQL.location_id,
    )

    # filter on regime
    if regime is not None:
        query = query.where(GSPYieldSQL.regime == regime)

    # filter on datetime
    query = query.where(GSPYieldSQL.datetime_utc >= start_datetime_utc)
    if end_datetime_utc is not None:
        query = query.where(GSPYieldSQL.datetime_utc <= end_datetime_utc)

    # don't get any nans. (Note nan+1 > nan = False)
    if filter_nans:
        query = query.where(GSPYieldSQL.solar_generation_kw + 1 > GSPYieldSQL.solar_generation_kw)

    # only select on results per gsp
    query = query.distinct(*[LocationSQL.gsp_id, GSPYieldSQL.datetime_utc])

    # select only the gsp systems we want
    query = query.where(LocationSQL.gsp_id.in_(gsp_ids))

    # order by 'created_utc' desc, so we get the latest one
    query = query.order_by(
        LocationSQL.gsp_id,
        desc(GSPYieldSQL.datetime_utc),
        desc(GSPYieldSQL.created_utc),
    )

    # get all results
    gsp_yields: List[GSPYieldSQL] = query.all()

    for gsp_yield in gsp_yields:
        gsp_yield.datetime_utc = gsp_yield.datetime_utc.replace(tzinfo=timezone.utc)

    return gsp_yields


def get_gsp_yield_by_location(
    session: Session,
    gsp_ids: List[int],
    start_datetime_utc: datetime,
    regime: Optional[str] = None,
    end_datetime_utc: Optional[datetime] = None,
    filter_nans: Optional[bool] = True,
) -> List[LocationSQL]:
    """
    Get the locations with gsp yield values.

    :param session: sqlalmcy sessions
    :param gsp_ids: list of gsp ids that we filter on
    :param start_datetime_utc: filter values on this start datetime
    :param regime: filter query on this regim. Can be "in-day" or "day-after"
    :param end_datetime_utc: optional end datetime filter
    :param filter_nans: optional filter out nans. Default is True
    :return: list of locations with gsp yields
    """

    # start main query
    query = session.query(LocationSQL)
    query = query.join(GSPYieldSQL)
    query = query.where(LocationSQL.id == GSPYieldSQL.location_id)

    # filter on regime
    if regime is not None:
        query = query.where(GSPYieldSQL.regime == regime)

    # filter on datetime
    query = query.where(GSPYieldSQL.datetime_utc >= start_datetime_utc)
    if end_datetime_utc is not None:
        query = query.where(GSPYieldSQL.datetime_utc <= end_datetime_utc)

    # don't get any nans. (Note nan+1 > nan = False)
    if filter_nans:
        query = query.where(GSPYieldSQL.solar_generation_kw + 1 > GSPYieldSQL.solar_generation_kw)

    # only select on results per gsp
    query = query.distinct(*[LocationSQL.gsp_id, GSPYieldSQL.datetime_utc])

    # select only the gsp systems we want
    query = query.where(LocationSQL.gsp_id.in_(gsp_ids))

    # load only once
    query = query.options(contains_eager(LocationSQL.gsp_yields))

    # order by 'created_utc' desc, so we get the latest one
    query = query.order_by(
        LocationSQL.gsp_id,
        desc(GSPYieldSQL.datetime_utc),
        desc(GSPYieldSQL.created_utc),
    )

    # get all results
    locations: List[LocationSQL] = query.all()

    for location in locations:
        for gsp_yield in location.gsp_yields:
            gsp_yield.datetime_utc = gsp_yield.datetime_utc.replace(tzinfo=timezone.utc)

    return locations


def get_gsp_yield_sum(
    session: Session,
    gsp_ids: List[int],
    start_datetime_utc: datetime,
    regime: Optional[str] = None,
    end_datetime_utc: Optional[datetime] = None,
) -> List[GSPYield]:
    """
    Get the sum of gsp yield values.

    :param session: sqlalchemy sessions
    :param gsp_ids: list of gsp ids that we filter on
    :param start_datetime_utc: filter values on this start datetime
    :param regime: filter query on this regim. Can be "in-day" or "day-after"
    :param end_datetime_utc: optional end datetime filter

    :return: list of GSPYield objects
    """

    logger.info(f"Getting gsp yield sum for {len(gsp_ids)} gsp systems")

    if regime is None:
        logger.debug("No regime given, defaulting to 'in-day'")
        regime = "in-day"

    # start main query
    query = session.query(
        GSPYieldSQL.datetime_utc,
        func.sum(GSPYieldSQL.solar_generation_kw).label("solar_generation_kw"),
    )

    # join with location table
    query = query.join(LocationSQL)

    # select only the gsp systems we want
    query = query.where(LocationSQL.gsp_id.in_(gsp_ids))

    # filter on regime
    query = query.where(GSPYieldSQL.regime == regime)

    # filter on datetime
    query = query.where(GSPYieldSQL.datetime_utc >= start_datetime_utc)
    if end_datetime_utc is not None:
        query = query.where(GSPYieldSQL.datetime_utc <= end_datetime_utc)

    # filter out nans
    query = query.where(GSPYieldSQL.solar_generation_kw + 1 > GSPYieldSQL.solar_generation_kw)

    # group and order by datetime
    query = query.group_by(GSPYieldSQL.datetime_utc)
    query = query.order_by(GSPYieldSQL.datetime_utc)

    results = query.all()

    # format results
    results = [
        GSPYield(
            datetime_utc=result.datetime_utc,
            solar_generation_kw=result.solar_generation_kw,
            regime=regime,
        )
        for result in results
    ]

    return results


def get_latest_gsp_capacities(
    session: Session, gsp_ids: List[int], datetime_utc: Optional[datetime] = None
) -> pd.Series:
    """
    Get the latest gsp capacities.

    :param session: database sessions
    :param gsp_ids: list of gsp_ids
    :param datetime_utc: datetime to filter on
    :return: pd.Series with gsp capacities, gsp_ids as the index
    """

    gsp_yields = get_latest_gsp_yield(session=session, gsps=gsp_ids, datetime_utc=datetime_utc)

    # format results
    eff_capacities = [gsp_yield.capacity_mwp for gsp_yield in gsp_yields]
    gsp_ids_from_db = [gsp_yield.location.gsp_id for gsp_yield in gsp_yields]

    return pd.Series(eff_capacities, index=gsp_ids_from_db)

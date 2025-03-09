""" Read user"""

import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy.orm import contains_eager
from sqlalchemy.orm.session import Session

from nowcasting_datamodel.models.api import APIRequestSQL, UserSQL

logger = logging.getLogger(__name__)


def get_user(session: Session, email: str) -> UserSQL:
    """
    Get metric object from database

    :param session: database session
    :param email: email of user
    return: One Metric SQl object
    """

    # start main query
    query = session.query(UserSQL)
    # filter on name
    query = query.filter(UserSQL.email == email)
    # get all results
    users = query.all()
    if len(users) == 0:
        logger.debug(f"User for name {email} does not exist so going to add it")
        user = UserSQL(email=email)
        session.add(user)
        session.commit()
    else:
        user = users[0]
    return user


def get_all_last_api_request(
    session: Session, include_in_url: Optional[str] = None, exclude_in_url: Optional[str] = None
) -> List[APIRequestSQL]:
    """
    Get all last api requests for all users

    :param session: database session
    :param include_in_url: Optional filter to include only URLs containing this string
    :param exclude_in_url: Optional filter to exclude URLs containing this string
    :return: List of last API requests
    """

    query = (
        session.query(APIRequestSQL)
        .distinct(APIRequestSQL.user_uuid)
        .join(UserSQL)
        .options(contains_eager(APIRequestSQL.user))
        .populate_existing()
        .order_by(APIRequestSQL.user_uuid, APIRequestSQL.created_utc.desc())
    )

    # Apply URL filtering if specified
    if include_in_url is not None:
        query = query.filter(APIRequestSQL.url.like(f"%{include_in_url}%"))

    if exclude_in_url is not None:
        query = query.filter(~APIRequestSQL.url.like(f"%{exclude_in_url}%"))

    return query.all()


def get_api_requests_for_one_user(
    session: Session,
    email: str,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    include_in_url: Optional[str] = None,
    exclude_in_url: Optional[str] = None,
) -> List[APIRequestSQL]:
    """
    Get all api requests for one user

    :param session: database session
    :param email: user email
    :param start_datetime: only get api requests after start datetime
    :param end_datetime: only get api requests before end datetime
    :param include_in_url: Optional filter to include only URLs containing this string
    :param exclude_in_url: Optional filter to exclude URLs containing this string
    """

    query = session.query(APIRequestSQL).join(UserSQL).filter(UserSQL.email == email)

    if start_datetime is not None:
        query = query.filter(APIRequestSQL.created_utc >= start_datetime)

    if end_datetime is not None:
        query = query.filter(APIRequestSQL.created_utc <= end_datetime)

    # Apply URL filtering if specified
    if include_in_url is not None:
        query = query.filter(APIRequestSQL.url.like(f"%{include_in_url}%"))

    if exclude_in_url is not None:
        query = query.filter(~APIRequestSQL.url.like(f"%{exclude_in_url}%"))

    return query.order_by(APIRequestSQL.created_utc.desc()).all()

""" Utils functions for models """

import logging
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


def datetime_with_timezone(cls, v: Any) -> Optional[datetime]:
    """Convert to a datetime with a timezone"""
    dt = v
    if not isinstance(dt, datetime):
        if dt is None:
            return dt
        elif isinstance(dt, str):
            dt = datetime.fromisoformat(dt)
        else:
            raise TypeError(f"argument must be a datetime or a str, but was {dt.type()}")
    return _datetime_must_have_timezone(cls, dt)


def _datetime_must_have_timezone(cls, v: datetime) -> Optional[datetime]:
    """Enforce that this variable must have a timezone"""
    if v is None:
        return v
    if v.tzinfo is None:
        logger.debug(f"{v} must have a timezone, for cls {cls}." f"So we are going to add UTC ")
        v = v.replace(tzinfo=timezone.utc)
    return v


def convert_to_camelcase(snake_str: str) -> str:
    """Converts a given snake_case string into camelCase"""
    first, *others = snake_str.split("_")
    return "".join([first.lower(), *map(str.title, others)])

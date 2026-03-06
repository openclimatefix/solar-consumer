import logging
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pvlib

logger = logging.getLogger(__name__)

ELEVATION_LIMIT = float(os.getenv("ELEVATION_LIMIT", "5"))

_dir = os.path.dirname(__file__)
_gsp_locations_file = os.path.join(_dir, "uk_gsp_locations.csv")
_gsp_locations = pd.read_csv(_gsp_locations_file)
_gsp_locations = _gsp_locations.set_index("gsp_id")


def _round_up_to_half_hour(dt: datetime) -> datetime:
    dt = dt.replace(microsecond=0)
    if dt.minute == 0 and dt.second == 0:
        return dt
    if dt.minute < 30 or (dt.minute == 30 and dt.second == 0):
        return dt.replace(minute=30, second=0)
    return dt.replace(minute=0, second=0) + timedelta(hours=1)


def make_night_time_zeros(
    start: datetime,
    end: datetime,
    gsp_id: int,
    regime: str,
) -> pd.DataFrame:
    """Return zero generation rows for nighttime periods."""
    empty = pd.DataFrame()

    if regime != "in-day":
        return empty

    if gsp_id not in _gsp_locations.index:
        logger.warning(f"GSP ID {gsp_id} not found in location file, skipping nighttime zeros")
        return empty

    gsp_location = _gsp_locations.loc[gsp_id]
    longitude = gsp_location["longitude"]
    latitude = gsp_location["latitude"]

    rounded_start = _round_up_to_half_hour(start)
    times = pd.date_range(start=rounded_start, end=end, freq="30min")

    if len(times) == 0:
        return empty

    solpos = pvlib.solarposition.get_solarposition(
        time=times, longitude=longitude, latitude=latitude, method="nrel_numpy"
    )
    elevation = solpos["elevation"]

    nighttime_mask = elevation < ELEVATION_LIMIT
    nighttime_times = elevation[nighttime_mask].index

    if len(nighttime_times) == 0:
        return empty

    now_utc = datetime.now(timezone.utc)

    gsp_yield_df = pd.DataFrame(
        {
            "generation_mw": 0.0,
            "datetime_gmt": nighttime_times,
            "installedcapacity_mwp": 0.0,
            "capacity_mwp": 0.0,
            "updated_gmt": now_utc,
        }
    )

    logger.info(
        f"Created {len(gsp_yield_df)} nighttime zero rows for GSP {gsp_id} "
        f"({rounded_start} to {end})"
    )

    return gsp_yield_df

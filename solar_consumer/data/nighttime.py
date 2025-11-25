# solar_consumer/data/nighttime.py
from __future__ import annotations

from typing import Optional, Union
import pandas as pd
import pvlib
from loguru import logger

# Import the preloaded GSP locations table
from .uk_gsp_locations import GSP_LOCATIONS as _GSP_LOCATIONS


def make_night_time_zeros(
    df: pd.DataFrame,
    *,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    gsp_id: Optional[int] = None,
    timestamp_col: str = "target_time_utc",
    generation_col: str = "generation_mw",
    elevation_limit_deg: Union[int, float] = 5,
    start: Optional[pd.Timestamp] = None,
    end: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Zero-out generation at night using solar elevation computed via pvlib.

    Night is defined as elevation < `elevation_limit_deg` (default 5°).
    This avoids needing `sunrise_utc` / `sunset_utc` columns.

    Parameters
    ----------
    df : DataFrame with at least [timestamp_col, generation_col]
    latitude, longitude : float, optional
        Site location (degrees). If not provided, and gsp_id is given, the function
        will try to look them up from the bundled GSP locations table.
    gsp_id : int, optional
        GSP identifier used to fetch lat/lon when not supplied explicitly.
    timestamp_col : str
        Timestamp column (UTC or tz-naive assumed UTC).
    generation_col : str
        Generation column to zero at night.
    elevation_limit_deg : int | float
        Threshold below which values are considered night.
    start, end : pandas.Timestamp, optional
        If df is empty, these are used to backfill a 30-minute time index.

    Returns
    -------
    DataFrame
        Copy of df with nighttime rows set to zero in `generation_col`.
    """
    # If no data, build a time index for the query window (backup)
    if (df is None or df.empty) and (start is not None and end is not None):
        times = pd.date_range(
            start=start, end=end, freq="30min", tz="UTC", inclusive="left"
        )
        df = pd.DataFrame(
            {
                timestamp_col: times,
                generation_col: 0.0,               # synthesized zero generation
                "installedcapacity_mwp": 0.0,      # default capacity fields
                "capacity_mwp": 0.0,
                "updated_gmt": pd.NaT,
            }
        )
        # keep gsp_id if provided
        if gsp_id is not None:
            df["gsp_id"] = gsp_id

    if df is None or df.empty:
        return df

    if timestamp_col not in df or generation_col not in df:
        return df

    # Fallback: if coords not provided, try to get them from the bundled CSV using gsp_id
    if (latitude is None or longitude is None) and gsp_id is not None:
        if _GSP_LOCATIONS is not None and gsp_id in _GSP_LOCATIONS.index:
            latitude = float(_GSP_LOCATIONS.at[gsp_id, "latitude"])
            longitude = float(_GSP_LOCATIONS.at[gsp_id, "longitude"])

    # If still no coordinates, skip zeroing gracefully
    if latitude is None or longitude is None:
        logger.debug(
            "No lat/lon available for night-time zeroing; leaving data unchanged."
        )
        return df

    out = df.copy()

    # Ensure UTC-aware datetime
    out[timestamp_col] = pd.to_datetime(out[timestamp_col], utc=True, errors="coerce")
    valid = out[timestamp_col].notna()
    if not valid.any():
        return out

    # Compute solar elevation for all timestamps
    solpos = pvlib.solarposition.get_solarposition(
        time=out.loc[valid, timestamp_col],
        latitude=latitude,
        longitude=longitude,
        method="nrel_numpy",
    )
    elevation = solpos["elevation"]

    # Night mask: elevation below threshold
    night_mask = elevation < float(elevation_limit_deg)

    # Apply zeros only on rows with valid timestamps
    idx = out.loc[valid].index[night_mask]
    out.loc[idx, generation_col] = 0.0

    return out
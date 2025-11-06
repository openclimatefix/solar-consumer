# solar_consumer/data/nighttime.py
from __future__ import annotations

from typing import Union, Optional
import pandas as pd
import pvlib
import os
from loguru import logger


# Load GSP lat/lon for night-time zeroing from CSV (no datamodel dependency)
DIR = os.path.dirname(__file__)


def _load_gsp_locations() -> pd.DataFrame | None:
    """Load GSP lat/lon if the CSV exists; return None otherwise."""
    candidates = [
        os.path.join(DIR, "uk_gsp_locations_20250109.csv"),
        os.path.join(DIR, "data", "uk_gsp_locations_20250109.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            df = pd.read_csv(path)
            if "gsp_id" in df.columns:
                return df.set_index("gsp_id")
    logger.warning("GSP locations CSV not found; skipping night-time zeroing.")
    return None


_GSP_LOCATIONS = _load_gsp_locations()

# NOTE:
# rather than computing irradiance or using cloud info,
# treat any hour where the sun elevation < ~5° as night.
# pvlib's solarposition is well validated; this is equivalent
# to OCF's metnet threshold but avoids needing extra columns.


def make_night_time_zeros(
    df: pd.DataFrame,
    *,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    gsp_id: Optional[int] = None,
    ts_col: str = "target_time_utc",
    mw_col: str = "generation_mw",
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
    df : DataFrame with at least [ts_col, mw_col]
    latitude, longitude : float
        GSP/site location (degrees).
    ts_col : str
        Timestamp column (UTC or tz-naive assumed UTC).
    mw_col : str
        Generation column to zero at night.
    elevation_limit_deg : int | float
        Threshold below which values are considered night.
    start : pandas.Timestamp | None, optional
        Accepted for API compatibility; not used by this function.
    end : pandas.Timestamp | None, optional
        Accepted for API compatibility; not used by this function.

    Returns
    -------
    DataFrame
        Copy of df with nighttime rows set to zero in `mw_col`.
    """
    # If no data, build a time index for the query window (backup)
    if (df is None or df.empty) and (start is not None and end is not None):
        times = pd.date_range(
            start=start, end=end, freq="30min", tz="UTC", inclusive="left"
        )
        df = pd.DataFrame({ts_col: times, mw_col: pd.NA})

    if df is None or df.empty:
        return df

    if ts_col not in df or mw_col not in df:
        return df
    # Fallback: if coords not provided, try to get them from the bundled CSV using gsp_id
    if (latitude is None or longitude is None) and gsp_id is not None:
        try:
            locs = (
                _GSP_LOCATIONS
                if "_GSP_LOCATIONS" in globals()
                else _load_gsp_locations()
            )
        except Exception:
            locs = None
        if locs is not None and gsp_id in locs.index:
            latitude = float(locs.at[gsp_id, "latitude"])
            longitude = float(locs.at[gsp_id, "longitude"])

    # If still no coordinates, skip zeroing gracefully
    if latitude is None or longitude is None:
        logger.debug(
            "No lat/lon available for night-time zeroing; leaving data unchanged."
        )
        return df
    out = df.copy()

    # Ensure UTC-aware datetime
    out[ts_col] = pd.to_datetime(out[ts_col], utc=True, errors="coerce")
    valid = out[ts_col].notna()
    if not valid.any():
        return out

    # Compute solar elevation for all timestamps
    solpos = pvlib.solarposition.get_solarposition(
        time=out.loc[valid, ts_col],
        latitude=latitude,
        longitude=longitude,
        method="nrel_numpy",
    )
    elevation = solpos["elevation"]

    # Night mask: elevation below threshold
    night_mask = elevation < float(elevation_limit_deg)

    # Apply zeros only on rows with valid timestamps
    idx = out.loc[valid].index[night_mask]
    out.loc[idx, mw_col] = 0.0

    return out

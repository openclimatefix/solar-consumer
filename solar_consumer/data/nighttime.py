# solar_consumer/data/nighttime.py
from __future__ import annotations

from typing import Union
import pandas as pd


# NOTE:
# rather than computing irradiance or using cloud info,
# treat any hour where the sun elevation < ~5° as night.
# pvlib's solarposition is well validated; this is equivalent
# to OCF's metnet threshold but avoids needing extra columns.

def make_night_time_zeros(
    df: pd.DataFrame,
    *,
    latitude: float,
    longitude: float,
    ts_col: str = "target_time_utc",
    mw_col: str = "generation_mw",
    elevation_limit_deg: Union[int, float] = 5,
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

    Returns
    -------
    DataFrame
        Copy of df with nighttime rows set to zero in `mw_col`.
    """
    try:
        import pvlib  # lazy import
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "pvlib is required for night-time zeroing. "
            "Install it with `pip install pvlib` or ensure CI installs it."
        ) from e

    if df is None or df.empty:
        return df

    if ts_col not in df or mw_col not in df:
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
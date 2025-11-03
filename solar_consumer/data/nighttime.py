# solar_consumer/pvlive/nighttime.py
from __future__ import annotations
import pandas as pd

def make_night_time_zeros(
    df: pd.DataFrame,
    ts_col: str = "target_time_utc",
    mw_col: str = "generation_mw",
    sunrise_col: str = "sunrise_utc",
    sunset_col: str = "sunset_utc",
) -> pd.DataFrame:
    """
    Set generation to 0 outside [sunrise_utc, sunset_utc).

    Expects gsp_yield_df to contain:
      - 'target_time_utc' (timestamps, ideally tz-aware UTC)
      - 'generation_mw' (numeric)
      - optional 'sunrise_utc' / 'sunset_utc' (timestamps). If missing, no-op.
    Returns a COPY of the dataframe with zeros applied.
    """
    if df is None or len(df) == 0:
        return df

    df = df.copy()

    # Must have the core columns
    if (ts_col not in df) or (mw_col not in df):
        return df

    # If sunrise/sunset not present yet, do nothing (safe no-op)
    if (sunrise_col not in df) or (sunset_col not in df):
        return df

    # Ensure datetime dtype (UTC-aware if possible)
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df[sunrise_col]     = pd.to_datetime(df[sunrise_col],     utc=True, errors="coerce")
    df[sunset_col]      = pd.to_datetime(df[sunset_col],      utc=True, errors="coerce")

    # Build mask: night is before sunrise OR at/after sunset
    at_night = (df[ts_col] < df[sunrise_col]) | (df[ts_col] >= df[sunset_col])
    df.loc[at_night, mw_col] = 0.0

    return df
# solar_consumer/pvlive/nighttime.py
from __future__ import annotations
import pandas as pd

def make_night_time_zeros(
    start,  # kept for signature compatibility (unused)
    end,    # kept for signature compatibility (unused)
    gsp_id, # kept for signature compatibility (unused)
    gsp_yield_df: pd.DataFrame,
    regime, # kept for signature compatibility (unused)
) -> pd.DataFrame:
    """
    Set generation to 0 outside [sunrise_utc, sunset_utc).

    Expects gsp_yield_df to contain:
      - 'target_time_utc' (timestamps, ideally tz-aware UTC)
      - 'generation_mw' (numeric)
      - optional 'sunrise_utc' / 'sunset_utc' (timestamps). If missing, no-op.
    Returns a COPY of the dataframe with zeros applied.
    """
    if gsp_yield_df is None or len(gsp_yield_df) == 0:
        return gsp_yield_df

    df = gsp_yield_df.copy()

    # Must have the core columns
    if ("target_time_utc" not in df) or ("generation_mw" not in df):
        return df

    # If sunrise/sunset not present yet, do nothing (safe no-op)
    if ("sunrise_utc" not in df) or ("sunset_utc" not in df):
        return df

    # Ensure datetime dtype (UTC-aware if possible)
    df["target_time_utc"] = pd.to_datetime(df["target_time_utc"], utc=True, errors="coerce")
    df["sunrise_utc"]     = pd.to_datetime(df["sunrise_utc"],     utc=True, errors="coerce")
    df["sunset_utc"]      = pd.to_datetime(df["sunset_utc"],      utc=True, errors="coerce")

    # Build mask: night is before sunrise OR at/after sunset
    at_night = (df["target_time_utc"] < df["sunrise_utc"]) | (df["target_time_utc"] >= df["sunset_utc"])
    df.loc[at_night, "generation_mw"] = 0.0

    return df
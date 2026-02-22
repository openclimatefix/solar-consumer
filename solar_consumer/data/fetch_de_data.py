import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from entsoe import EntsoePandasClient
from loguru import logger

# German bidding zone
DE_TSO_ZONE = "10Y1001A1001A82H"

def fetch_de_data_range(start: datetime, end: datetime, chunk_hours: int = 168):
    """    
    Fetch German solar generation over a date range by chunking into windows (smaller payloads for API
    and more robust behaviour for large ranges).
    - start/end: inclusive start/exclusive end datetime (UTC)
    - chunk_hours: window size, default 168h (or 7 days)

    Returns DataFrame with columns:
      - target_datetime_utc (UTC)
      - solar_generation_kw (kW)
      - tso_zone
    """

    # API access handled by entsoe-py, not XML
    api_key = os.getenv("ENTSOE_API_KEY")
    if not api_key:
        raise RuntimeError("WARNING: ENTSOE_API_KEY not set in environment")

    assert start < end, "Start date must be before end"

    # Normalise to UTC and hour boundaries
    def norm(t):
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        else:
            t = t.astimezone(timezone.utc)
        return t.replace(minute=0, second=0, microsecond=0)
    
    start = norm(start)
    end = norm(end)

    client = EntsoePandasClient(api_key=api_key)

    frames = []
    window = start
    step = timedelta(hours=chunk_hours)

    # Fetch one window from the ENTSOE API and collect non-empty solar gen results
    while window < end:
        nxt = min(window + step, end)

        # entsoe-py request (generation in MW)
        gen_mw = client.query_generation(
            country_code="DE",
            start=pd.Timestamp(window),
            end=pd.Timestamp(nxt),
            psr_type="Solar",
        )

        # Convert to standard schema (UTC + kW)
        if gen_mw is not None and not gen_mw.empty:
            idx = pd.to_datetime(gen_mw.index)

            # Ensure tz-aware UTC stamps
            if getattr(idx, "tz", None) is None:
                idx = idx.tz_localize("UTC")
            else:
                idx = idx.tz_convert("UTC")

            df_chunk = pd.DataFrame(
                {
                    "target_datetime_utc": idx,
                    "solar_generation_kw": (gen_mw.astype(float) * 1000.0).to_numpy(),
                    "tso_zone": DE_TSO_ZONE,
                }
            )
            df_chunk = df_chunk.sort_values("target_datetime_utc").reset_index(drop=True)
            frames.append(df_chunk)

        window = nxt

    # If all windows are completely empty, return an empty one with the right shape
    if not frames:
        return pd.DataFrame(
            columns=["target_datetime_utc", "solar_generation_kw", "tso_zone"]
        )

    # Concatenate to a single table
    df = pd.concat(frames, ignore_index=True)
    df = (
        df.drop_duplicates(subset=["target_datetime_utc", "tso_zone"])
        .sort_values("target_datetime_utc")
        .reset_index(drop=True)
    )
    logger.info("Assembled {} rows of German solar data over range.", len(df))
    return df


def fetch_de_data(historic_or_forecast: str = "generation"):
    """
    Fetch solar generation data from German bidding zones via the
    ENTSOE API (24 HOUR FETCH)

    Only 'generation' mode is supported for now

    Returns DataFrame with 3 columns:
      - target_datetime_utc (UTC date and time)
      - solar_generation_kw (generation in kilowatts)
      - tso_zone (bidding zone code)
    """
    
    assert (
        historic_or_forecast == "generation"
    ), "Only 'generation' supported for the time being"

    # Fetches data from last 24 hours from current time
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(hours=24)

    # Keep behaviour stable: use the same range fetch and schema conversion path
    df = fetch_de_data_range(start, now, chunk_hours=24)
    logger.info("Assembled {} rows of German solar data", len(df))
    return df

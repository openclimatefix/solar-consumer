import os
import pandas as pd
import dotenv
from datetime import datetime, timedelta, timezone
import requests
import xml.etree.ElementTree as ET
from loguru import logger

# Load environment variables
dotenv.load_dotenv()

# Prepare request
URL = "https://web-api.tp.entsoe.eu/api" # base URL for api
API_KEY = os.getenv("ENTSOE_API_KEY", "") # api key from env vars, empty string if missing
SOLAR_PSR_CODES = {"B16", "A-10Y1001A1001A83H"}  # accept both real-world and test solar codes


def _fetch_de_window(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch German solar gen data from ENTSOE for specific time window (UTC) (>24H FETCH)
    
    It should be noted there are no resampling or filling gaps.
    Returns DataFrame with columns:
      - target_datetime_utc (UTC)
      - solar_generation_kw (kW)
      - tso_zone
    """
    
    if not API_KEY:
        raise RuntimeError("WARNING: ENTSOE_API_KEY not set in environment")

    # Sanity check for date request
    assert start < end, "Start date must be before end"
    period_start = start.strftime("%Y%m%d%H%M")
    period_end = end.strftime("%Y%m%d%H%M")

    params = {
        "documentType": "A75",
        "processType": "A16",
        "in_Domain": "10Y1001A1001A83F",
        "psrType": "B16",
        "periodStart": period_start,
        "periodEnd": period_end,
        "securityToken": API_KEY,
    }

    session = requests.Session()
    response = session.get(URL, params= params)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.error("API request failed- {}: {}", response.status_code, e)
        raise

    # Parse XML response
    root = ET.fromstring(response.content)
    records = []

    # For each TimeSeries (ts), extract the TSO and only solar PSR
    for ts in root.findall(".//TimeSeries"):
        zone = ts.findtext(".//inBiddingZone_Domain/Mrid")
        psr = ts.findtext(".//MktPSRType/psrType")
        if psr not in SOLAR_PSR_CODES:
            continue

        # Get each timestamped value in each ts
        for pt in ts.findall(".//Period/Point"):
            start_str = pt.findtext("timeInterval/start")
            qty_str = pt.findtext("quantity")
            try:
                qty = float(qty_str)
            except (TypeError, ValueError):
                logger.error("Skipping malformed entry in response: ({}) in zone {}", qty_str, zone)
                continue

            dt = pd.to_datetime(start_str, utc=True)
            
            records.append(
                {
                    "target_datetime_utc": dt,
                    "solar_generation_kw": qty * 1000.0,
                    "tso_zone": zone,
                }
            )

    # Create time-ordered dataframe and return completed window
    # (multi-window calls will be concatenated by caller)
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("target_datetime_utc").reset_index(drop = True)
    return df


def fetch_de_data_range(start: datetime, end: datetime, chunk_hours: int = 168) -> pd.DataFrame:
    """
    Fetch German solar generation over a date range by chunking into windows (smaller payloads for API 
    and more robust 'retry" options)
    - start/end: inclusive start/exclusive end datetime (UTC expected)
    - chunk_hours: window size (default 168h = 7 days) to keep payloads reasonable.
    Returns a DataFrame with the same schema as _fetch_de_window
    """
    assert start < end, "Start date must be before end"

    # Normalise to UTC and hour boundaries
    if start.tzinfo is None:
        start = start.replace(tzinfo = timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo = timezone.utc)
    start = start.replace(minute = 0, second = 0, microsecond = 0)
    end = end.replace(minute = 0, second = 0, microsecond = 0)

    # Accumulate windows to concat at end
    frames = []
    window = start
    step = timedelta(hours = chunk_hours)

    # Fetch one window (network and XML parse) from start to end, and store non-empty results
    while window < end:
        nxt = min(window + step, end)
        df_chunk = _fetch_de_window(window, nxt)
        if not df_chunk.empty:
            frames.append(df_chunk)
        window = nxt

    # If all windows are completly empty, return empty with right shape
    if not frames:
        return pd.DataFrame(columns=["target_datetime_utc", "solar_generation_kw", "tso_zone"])

    # Concatenate to a single table
    df = pd.concat(frames, ignore_index = True)
    df = (df.drop_duplicates(subset=["target_datetime_utc", "tso_zone"]).sort_values("target_datetime_utc")
          .reset_index(drop=True))
    logger.info("Assembled {} rows of German solar data over range.", len(df))
    return df


def fetch_de_data(historic_or_forecast: str = "generation") -> pd.DataFrame:
    """
    Fetch solar generation data from German bidding zones via the
    ENTSOE API (24 HOUR FETCH)

    Only 'generation' mode is supported for now
    
    Returns DataFrame with 3 columns:
      - target_datetime_utc (UTC date and time)
      - solar_generation_kw (generation in kilowatts)
      - tso_zone (bidding zone code)
    """
    
    assert historic_or_forecast == "generation", "Only 'generation' supported for the time being"

    # Fetches data from last 24 hours from current time
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(hours=24)
    period_start = start.strftime("%Y%m%d%H%M")
    period_end = now.strftime("%Y%m%d%H%M")

    params = {
        "documentType": "A75",    # actual generation
        "processType": "A16",     # realised output
        "in_Domain": "10Y1001A1001A83F",
        "psrType": "B16",
        "periodStart": period_start,
        "periodEnd": period_end,
        "securityToken": API_KEY,
    }

    # Initialise session for request
    session = requests.Session()
    logger.debug("Requesting German data from API with params: {}", params)
    response = session.get(URL, params=params)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.error("API request failed, {}: {}", response.status_code, e)
        raise    
    logger.error(f"Bytes: {len(response.content)}")

    # Parse XML
    root = ET.fromstring(response.content)
    records = []

    # Each <TimeSeries> represents one tso zone and one energy type
    for ts in root.findall(".//TimeSeries"):
        zone = ts.findtext(".//inBiddingZone_Domain/Mrid")
        psr = ts.findtext(".//MktPSRType/psrType")
        if psr not in SOLAR_PSR_CODES: # Skips all non-solar data
            continue

        for pt in ts.findall(".//Period/Point"):
            start_str = pt.findtext("timeInterval/start")
            qty_str = pt.findtext("quantity") # Quantity is in MW, converted to kW later
            try:
                qty = float(qty_str)
            except (TypeError, ValueError):
                logger.error("Skipping malformed quantity ({}) in zone {}", qty_str, zone)
                continue

            # Convert and record in list
            dt = pd.to_datetime(start_str, utc=True)
            records.append({
                "target_datetime_utc": dt,
                "solar_generation_kw": qty * 1000,
                "tso_zone": zone,
            })

    # Build and tidy DataFrame
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("target_datetime_utc").reset_index(drop=True)
    
    logger.info("Assembled {} rows of German solar data", len(df))

    return df


if __name__ == "__main__":
    # Backfill last 5 years - yesterday and write to CSV
    output_dir = os.path.join("data", "de_solar")
    os.makedirs(output_dir, exist_ok = True)
    out_path = os.path.join(output_dir, "germany_solar_generation.csv")

    now_utc = datetime.now(timezone.utc).replace(minute = 0, second = 0, microsecond = 0)
    end = (now_utc - timedelta(days=1)).replace(minute = 0, second = 0, microsecond = 0)
    
    # Start at first day of the month 5 years ago for clean boundaries
    past_five_years = end - timedelta(days = 5 * 365)
    start = past_five_years.replace(day = 1, hour = 0, minute = 0, second = 0, microsecond = 0)

    # Perform backfill using week-long chunks as stated before
    df = fetch_de_data_range(start, end, chunk_hours = 168) ### Adjust if you hit API limits ###

    # Write to file (done with temp to avoid partial files)
    temp = out_path + ".tmp"
    df.to_csv(temp, index = False)
    os.replace(temp, out_path)
    logger.info("FINISHED: WROTE {} ROWS OF SOLAR GENERATION DATA TO FILE: {}", len(df), out_path)
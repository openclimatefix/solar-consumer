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
if not API_KEY:
    raise RuntimeError("WARNING: ENTSOE_API_KEY not set in environment")


def _fetch_de_window(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Fetch German solar gen data from ENTSOE for specific time window (UTC) (>24H FETCH)
    
    It should be noted there are no resampling or filling gaps.
    Returns DataFrame with columns:
      - target_datetime_utc (UTC)
      - solar_generation_kw (kW)
      - tso_zone
      
    """
    
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
        if psr and psr != "B16":
            continue

        # Get each timestamped value in each ts
        for pt in ts.findall(".//Period/Point"):
            start_str = pt.findtext("timeInterval/start")
            qty_str = pt.findtext("quantity")  # MW
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


def fetch_de_data(historic_or_forecast: str = "generation") -> pd.DataFrame:
    """
    Fetch solar generation data from German bidding zones via the
    ENTSOE API

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
        if psr != "A-10Y1001A1001A83H": # Skips all non-solar data
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
    print("Wrote to CSV")
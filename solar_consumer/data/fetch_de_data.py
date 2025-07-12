import os
import pandas as pd
import dotenv
from datetime import datetime, timedelta, timezone
import requests
import xml.etree.ElementTree as ET
from loguru import logger

# Load environment variables
dotenv.load_dotenv()

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
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    period_start = start.strftime("%Y%m%d%H%M")
    period_end = now.strftime("%Y%m%d%H%M")

    # Prepare request
    url = "https://web-api.tp.entsoe.eu/api" # base url for api
    api_key = os.getenv("ENTSOE_API_KEY", "") # api key from env vars, empty string if missing
    params = {
        "documentType": "A75",    # actual generation
        "processType": "A16",     # realised output
        "periodStart": period_start,
        "periodEnd": period_end,
        "securityToken": api_key,
    }

    # Initialise session for request
    session = requests.Session()
    logger.debug("Requesting German data from API with params: {}", params)
    response = session.get(url, params=params)
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
                logger.warning("Skipping malfromed quantity (%s) in zone %s", qty_str, zone)
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

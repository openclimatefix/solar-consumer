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
URL = "https://web-api.tp.entsoe.eu/api"  # base URL for api
API_KEY = os.getenv(
    "ENTSOE_API_KEY", ""
)  # api key from env vars, empty string if missing
SOLAR_PSR_CODES = {
    "B16",
    "A-10Y1001A1001A83H",
}  # accept both real-world and test solar codes


def _fetch_de_window(start: datetime, end: datetime):
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
        "in_Domain": "10Y1001A1001A82H",
        "psrType": "B16",
        "periodStart": period_start,
        "periodEnd": period_end,
        "securityToken": API_KEY,
    }

    session = requests.Session()
    response = session.get(URL, params=params)
    try:
        response.raise_for_status()
    except Exception as e:
        logger.error("API request failed- {}: {}", response.status_code, e)
        raise

    # Parse XML response
    root = ET.fromstring(response.content)
    records = []

    # API namespaces tags whereas test XML doesn't, so prefixed version is tried first
    # and defaults to plain tags if API-made prefix missing
    ns = {"genload": root.tag.split("}")[0].strip("{")}

    # Each <TimeSeries> represents one tso zone and one energy type
    for ts in root.findall(".//genload:TimeSeries", ns):

        # Get bidding zone code
        zone = ts.findtext(".//genload:inBiddingZone_Domain.mRID", namespaces=ns)

        # PSR type filtered for only solar (B16)
        psr = ts.findtext(".//genload:MktPSRType/genload:psrType", namespaces=ns)
        if psr and psr not in SOLAR_PSR_CODES:
            continue

        # Period/Point loop (each Point is one timestamped measurement)
        points = ts.findall(".//genload:Period/genload:Point", ns)
        for pt in points:

            # Convert generation quantity in MW to float
            qty_str = pt.findtext("genload:quantity", namespaces=ns)
            try:
                qty = float(qty_str)
            except (TypeError, ValueError):
                logger.error(
                    "Skipped bad entry: Response '{}' in Zone '{}'", qty_str, zone
                )
                continue

            # Get timestamp
            start_str = pt.findtext("genload:timeInterval/genload:start", namespaces=ns)

            # Obtain explicit timestamp per point if present
            if start_str:
                utc_ts = pd.to_datetime(start_str, utc=True)

            # If no explicit timestamps present, infer from Period and Point info
            else:
                period = ts.find(".//genload:Period", ns)
                if period is None:
                    logger.error("Skipped point: missing Period in Zone '{}'", zone)
                    continue

                # Gets Period start, resolution and Point position
                period_start = period.findtext(
                    "genload:timeInterval/genload:start", namespaces=ns
                )
                period_start = pd.to_datetime(period_start, utc=True)

                resolution = period.findtext("genload:resolution", namespaces=ns)

                position = pt.findtext("genload:position", namespaces=ns)
                pos = int(position)

                # If no info present, timestamp cannot be inferred
                if not (period_start and resolution and position):
                    logger.error("Skipped point: missing time in Zone '{}'", zone)
                    continue

                # Work out timestamp by taking period start and adding (pos - 1) steps of resolution
                # 1-indexed so offset pos by 1
                if resolution.startswith("PT") and resolution.endswith("M"):
                    minutes = int(resolution[2:-1])
                    offset = pd.Timedelta(minutes=minutes * (pos - 1))
                elif resolution in {"PT1H", "PT60M"}:
                    offset = pd.Timedelta(hours=pos - 1)
                else:
                    offset = pd.to_timedelta(resolution) * (pos - 1)

                # Combine period start with offset to get exact UTC timestamp
                utc_ts = period_start + offset

            # Store parsed record
            records.append(
                {
                    "target_datetime_utc": utc_ts,
                    "solar_generation_kw": qty * 1000.0,
                    "tso_zone": zone,
                }
            )

    # Create time-ordered dataframe and return completed window
    # (multi-window calls will be concatenated by caller)
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("target_datetime_utc").reset_index(drop=True)
    return df


def fetch_de_data_range(start: datetime, end: datetime, chunk_hours: int = 168):
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
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    start = start.replace(minute=0, second=0, microsecond=0)
    end = end.replace(minute=0, second=0, microsecond=0)

    # Accumulate windows to concat at end
    frames = []
    window = start
    step = timedelta(hours=chunk_hours)

    # Fetch one window (network and XML parse) from start to end, and store non-empty results
    while window < end:
        nxt = min(window + step, end)
        df_chunk = _fetch_de_window(window, nxt)
        if not df_chunk.empty:
            frames.append(df_chunk)
        window = nxt

    # If all windows are completly empty, return empty with right shape
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
    period_start = start.strftime("%Y%m%d%H%M")
    period_end = now.strftime("%Y%m%d%H%M")

    params = {
        "documentType": "A75",  # actual generation
        "processType": "A16",  # realised output
        "in_Domain": "10Y1001A1001A82H",
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

    # Parse XML response
    root = ET.fromstring(response.content)
    records = []

    # API namespaces tags whereas test XML doesn't, so prefixed version is tried first
    # and defaults to plain tags if API-made prefix missing
    ns = {"genload": root.tag.split("}")[0].strip("{")}

    # Each <TimeSeries> represents one tso zone and one energy type
    for ts in root.findall(".//genload:TimeSeries", ns):

        # Get bidding zone code
        zone = ts.findtext(".//genload:inBiddingZone_Domain.mRID", namespaces=ns)

        # PSR type filtered for only solar (B16)
        psr = ts.findtext(".//genload:MktPSRType/genload:psrType", namespaces=ns)
        if psr and psr not in SOLAR_PSR_CODES:
            continue

        # Period/Point loop (each Point is one timestamped measurement)
        points = ts.findall(".//genload:Period/genload:Point", ns)
        for pt in points:

            # Convert generation quantity in MW to float
            qty_str = pt.findtext("genload:quantity", namespaces=ns)
            try:
                qty = float(qty_str)
            except (TypeError, ValueError):
                logger.error(
                    "Skipped bad entry: Response '{}' in Zone '{}'", qty_str, zone
                )
                continue

            # Get timestamp
            start_str = pt.findtext("genload:timeInterval/genload:start", namespaces=ns)

            # Obtain explicit timestamp per point if present
            if start_str:
                utc_ts = pd.to_datetime(start_str, utc=True)

            # If no explicit timestamps present, infer from Period and Point info
            else:
                period = ts.find(".//genload:Period", ns)
                if period is None:
                    logger.error("Skipped point: missing Period in Zone '{}'", zone)
                    continue

                # Gets Period start, resolution and Point position
                period_start = period.findtext(
                    "genload:timeInterval/genload:start", namespaces=ns
                )
                period_start = pd.to_datetime(period_start, utc=True)

                resolution = period.findtext("genload:resolution", namespaces=ns)

                position = pt.findtext("genload:position", namespaces=ns)
                pos = int(position)

                # If no info present, timestamp cannot be inferred
                if not (period_start and resolution and position):
                    logger.error("Skipped point: missing time in Zone '{}'", zone)
                    continue

                # Work out timestamp by taking period start and adding (pos - 1) steps of resolution
                # 1-indexed so offset pos by 1
                if resolution.startswith("PT") and resolution.endswith("M"):
                    minutes = int(resolution[2:-1])
                    offset = pd.Timedelta(minutes=minutes * (pos - 1))
                elif resolution in {"PT1H", "PT60M"}:
                    offset = pd.Timedelta(hours=pos - 1)
                else:
                    offset = pd.to_timedelta(resolution) * (pos - 1)

                # Combine period start with offset to get exact UTC timestamp
                utc_ts = period_start + offset

            # Store parsed record
            records.append(
                {
                    "target_datetime_utc": utc_ts,
                    "solar_generation_kw": qty * 1000.0,
                    "tso_zone": zone,
                }
            )

    # Build and tidy DataFrame
    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("target_datetime_utc").reset_index(drop=True)

    logger.info("Assembled {} rows of German solar data", len(df))

    return df

import requests
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def _build_session() -> requests.Session:
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    return session

BASE_URL_FORECAST = (
    "https://opendata.elia.be/api/explore/v2.1/"
    "catalog/datasets/ods032/records"
)

BASE_URL_GENERATION = (
    "https://opendata.elia.be/api/explore/v2.1/"
    "catalog/datasets/ods087/records"
)

REQUEST_LIMIT = 50


def fetch_be_data(historic_or_forecast: str = "forecast") -> pd.DataFrame:
    """
    Fetch Belgian solar PV data from the Elia Open Data API.
    
    This unified function retrieves either forecast or generation data
    and returns it in the standard format used by other countries.

    Parameters:
        historic_or_forecast (str): "forecast" for forecast data, 
                                   anything else for generation data (default: "forecast")

    Returns:
        pd.DataFrame with columns:
          - target_datetime_utc: Timestamp in UTC
          - solar_generation_kw: Solar generation in kW
    """
    if historic_or_forecast == "forecast":
        return fetch_be_data_forecast()
    else:
        return fetch_be_data_generation()


def _fetch_records_time_window(
    start_utc: datetime,
    end_utc: datetime,
    base_url: str = BASE_URL_FORECAST,
) -> list[dict]:
    
    """
    Fetch records from the Elia Open Data API within a fixed datetime window
    using cursor-based pagination.

    This avoids offset-based pagination limits by paging backwards in time
    using the `datetime` field.

    A small backward step (1 second) is applied to the cursor on each
    iteration to guarantee progress and avoid infinite loops when multiple
    records share the same timestamp.
    """

    session = _build_session()

    all_records: list[dict] = []
    cursor = end_utc
    prev_cursor = None
    
    while True:
        params = {
            "limit": REQUEST_LIMIT,
            "order_by": "datetime desc",
            "where": (
                f'datetime >= "{start_utc.isoformat()}" '
                f'AND datetime <= "{cursor.isoformat()}"'
            ),
        }

        logger.debug("Fetching Elia BE data with params {}", params)

        try:
            response = session.get(base_url, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.ReadTimeout:
            logger.warning("Timeout hit, retrying window ending at {}", cursor)
            continue

        payload = response.json()
        records = payload.get("results", [])

        if not records:
            break
         
        # Records are returned in reverse chronological order (newest → oldest)
        # due to `order_by=datetime desc`, so we page backwards in time.
        all_records.extend(records)

        last_datetime = records[-1].get("datetime")
        if not last_datetime:
            break

        cursor = (
            pd.to_datetime(last_datetime, utc=True)
            - pd.Timedelta(seconds=1)
        )

        if prev_cursor is not None and cursor >= prev_cursor:
            logger.warning("Cursor stalled at {}, stopping", cursor)
            break

        prev_cursor = cursor

        if cursor < start_utc:
            break

    logger.info("Fetched {} Elia records", len(all_records))
    return all_records


def _process_be_data(
    raw_records: list[dict],
    generation_field: str,
    forecast_type: str,
    data_type: str,
) -> pd.DataFrame:
    """
    Common processing logic for both forecast and generation data.
    
    Parameters:
        raw_records: Raw API records
        generation_field: Field name to use for solar_generation_kw
        forecast_type: Value for forecast_type column
        data_type: Description for logging ("forecast" or "generation")
    """
    if not raw_records:
        logger.warning("No Belgian {} data returned from Elia API", data_type)
        return pd.DataFrame(
            columns=[
                "target_datetime_utc",
                "solar_generation_kw",
                "region",
                "forecast_type",
                "capacity_kw",
            ]
        )

    df = pd.DataFrame(raw_records)
    df["target_datetime_utc"] = pd.to_datetime(
        df["datetime"], utc=True, errors="coerce"
    )
    df["solar_generation_kw"] = df[generation_field] * 1000
    df["forecast_type"] = forecast_type
    df["capacity_kw"] = df["monitoredcapacity"] * 1000
    df["region"] = df["region"].astype(str).str.strip().str.lower()

    df = df.dropna(
        subset=[
            "target_datetime_utc",
            "solar_generation_kw",
            "region",
            "forecast_type",
            "capacity_kw",
        ]
    )

    df = df[
        [
            "target_datetime_utc",
            "solar_generation_kw",
            "region",
            "forecast_type",
            "capacity_kw",
        ]
    ]

    df = df.sort_values("target_datetime_utc").reset_index(drop=True)

    logger.info(
        "Assembled {} rows of Belgian solar {} data across {} regions",
        len(df),
        data_type,
        df["region"].nunique(),
    )

    return df


def fetch_be_data_forecast(days: int = 1) -> pd.DataFrame:
    """
    Fetch Belgian solar PV forecast data (national + regional)
    from the Elia Open Data API.

    This function retrieves forecast data for a rolling time window
    (default: last 1 day) ending at the current UTC time. Both national
    ("Belgium") and regional forecasts are included.

    The rolling-window approach avoids API pagination limits
    and is suitable for scheduled ingestion jobs.

    Parameters:
        days (int): Number of days to look back from now (default: 1)

    Returns:
        pd.DataFrame with columns:
          - target_datetime_utc: Forecast timestamp in UTC
          - solar_generation_kw: Forecast solar generation in kW
          - region: Region name (Belgium or sub-region)
          - forecast_type: Forecast type identifier
          - capacity_mwp: Monitored capacity in MWp
    """
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=days)

    raw_records = _fetch_records_time_window(
        start_utc=start_utc,
        end_utc=end_utc,
        base_url=BASE_URL_FORECAST,
    )

    return _process_be_data(
        raw_records,
        generation_field="mostrecentforecast",
        forecast_type="most_recent",
        data_type="forecast",
    )


def fetch_be_data_generation(
    days: int = 1
) -> pd.DataFrame:
    """
    Fetch Belgian solar PV generation data (national + regional)
    from the Elia Open Data API (realtime data).

    This function retrieves generation data for a rolling time window
    (default: last 1 day) ending at the current UTC time. Both national
    ("Belgium") and regional generation data are included.

    The rolling-window approach avoids API pagination limits
    and is suitable for scheduled ingestion jobs.

    Parameters:
        days (int): Number of days to look back from now (default: 1)

    Returns:
        pd.DataFrame with columns:
          - target_datetime_utc: Generation timestamp in UTC
          - solar_generation_kw: Solar generation in kW
          - region: Region name (Belgium or sub-region)
          - forecast_type: Forecast type identifier
          - capacity_mwp: Monitored capacity in MWp
    """
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=days)

    raw_records = _fetch_records_time_window(
        start_utc=start_utc,
        end_utc=end_utc,
        base_url=BASE_URL_GENERATION,
    )

    return _process_be_data(
        raw_records,
        generation_field="realtime",
        forecast_type="generation",
        data_type="generation",
    )
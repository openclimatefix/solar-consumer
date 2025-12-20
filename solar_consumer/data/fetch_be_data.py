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

BASE_URL = (
    "https://opendata.elia.be/api/explore/v2.1/"
    "catalog/datasets/ods032/records"
)

REQUEST_LIMIT = 50


def _fetch_records_time_window(
    start_utc: datetime,
    end_utc: datetime,
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
            response = session.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.ReadTimeout:
            logger.warning("Timeout hit, retrying window ending at {}", cursor)
            continue

        payload = response.json()
        records = payload.get("results", [])

        if not records:
            break

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

def fetch_be_data_forecast() -> pd.DataFrame:
    """
    Fetch Belgian solar PV forecast data (national + regional)
    from the Elia Open Data API.

    This function retrieves forecast data for a rolling 7-day
    window ending at the current UTC time. Both national
    ("Belgium") and regional forecasts are included.

    The rolling-window approach avoids API pagination limits
    and is suitable for scheduled ingestion jobs.

    Returns:
        pd.DataFrame with columns:
          - target_datetime_utc: Forecast timestamp in UTC
          - solar_generation_kw: Forecast solar generation in kW
          - region: Region name (Belgium or sub-region)
          - forecast_type: Forecast type identifier
    """
    end_utc = datetime.now(timezone.utc)
    start_utc = end_utc - timedelta(days=7)

    raw_records = _fetch_records_time_window(
        start_utc=start_utc,
        end_utc=end_utc,
    )

    if not raw_records:
        logger.warning("No Belgian forecast data returned from Elia API")
        return pd.DataFrame(
            columns=[
                "target_datetime_utc",
                "solar_generation_kw",
                "region",
                "forecast_type",
            ]
        )

    df = pd.DataFrame(raw_records)

    # Parse datetime
    df["target_datetime_utc"] = pd.to_datetime(
        df["datetime"], utc=True, errors="coerce"
    )

    # Convert MW -> kW
    df["solar_generation_kw"] = df["mostrecentforecast"] * 1000

    # Metadata
    df["forecast_type"] = "most_recent"

    # Drop invalid rows
    df = df.dropna(
        subset=[
            "target_datetime_utc",
            "solar_generation_kw",
            "region",
        ]
    )

    df = df[
        [
            "target_datetime_utc",
            "solar_generation_kw",
            "region",
            "forecast_type",
        ]
    ]

    df = df.sort_values("target_datetime_utc").reset_index(drop=True)

    logger.info(
        "Assembled {} rows of Belgian solar forecast data "
        "across {} regions",
        len(df),
        df["region"].nunique(),
    )

    return df

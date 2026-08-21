"""Get German solar generation and forecasts from the ENTSO-E.

Generation is published for each German TSO control area and Germany overall, 
enabling both regional and national values to be saved to the data platform.
Forecasts are national, and ENTSO-E publishes three of them: day ahead, intraday and current.
"""

import os

import dotenv
import pandas as pd
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from loguru import logger

from solar_consumer.constants import (
    DE_AREAS,
    DE_FORECAST_PROCESS_TYPES,
    DE_SOLAR_PSR_TYPE,
    DE_TSO_NAMES,
)

# Load environment variables
dotenv.load_dotenv()


def get_entsoe_client() -> EntsoePandasClient:
    """Make an ENTSO-E client, using the api key from the environment."""
    api_key = os.getenv("APIKEY_ENTSOE")
    if not api_key:
        raise ValueError(
            "No ENTSO-E api key found. Please set APIKEY_ENTSOE to fetch German data."
        )
    return EntsoePandasClient(api_key=api_key)


def get_de_forecast_type() -> str:
    """Get the type of forecast to collect: day_ahead, intraday or current."""
    forecast_type = os.getenv("DE_FORECAST_TYPE", "day_ahead").lower()
    if forecast_type not in DE_FORECAST_PROCESS_TYPES:
        raise ValueError(
            f"Unknown DE forecast type '{forecast_type}', "
            f"expected one of {list(DE_FORECAST_PROCESS_TYPES)}"
        )
    return forecast_type


def fetch_de_data(historic_or_forecast: str = "generation") -> pd.DataFrame:
    """Fetch German solar forecast from the ENTSO-E API."""
    if historic_or_forecast == "forecast":
        return fetch_de_data_forecast()

    return fetch_de_data_generation()


def fetch_de_data_generation() -> pd.DataFrame:
    """
    Fetch solar generation from the German control areas (TSOs) and the national
    total via the ENTSO-E API.
    
    Returns DataFrame with 5 columns:
      - target_datetime_utc (UTC date and time)
      - solar_generation_kw (generation in kilowatts)
      - capacity_kw (installed solar capacity in kilowatts, from ENTSO-E)
      - region (data platform join key, e.g. "de" or "50hertz")
      - tso_zone (TSO name, e.g. "50Hertz", or None for the national values)
    """

    # ENTSO-E only publishes settled values, so we always pull a window of recent history and
    # let the save step drop anything we already have.
    backfill_hours = int(os.getenv("DE_BACKFILL_HOURS", "24"))

    end = pd.Timestamp.now(tz="UTC").floor("h")
    start = end - pd.Timedelta(hours=backfill_hours)

    logger.info(f"Fetching German solar generation from {start} to {end}")

    client = get_entsoe_client()

    all_data = []
    for region, area in DE_AREAS.items():
        generation = _fetch_area_generation(client=client, area=area, start=start, end=end)

        if generation.empty:
            logger.warning(f"No German solar generation returned for {region} ({area})")
            continue

        generation["capacity_kw"] = _get_installed_capacity_kw(client=client, region=region, area=area)
        generation["region"] = region
        generation["tso_zone"] = DE_TSO_NAMES.get(region)
        all_data.append(generation)

    if not all_data:
        logger.warning("No German solar generation data found")
        return pd.DataFrame(
            columns=[
                "target_datetime_utc",
                "solar_generation_kw",
                "capacity_kw",
                "region",
                "tso_zone",
            ]
        )

    df = pd.concat(all_data, ignore_index=True)
    df = df.sort_values(["target_datetime_utc", "region"]).reset_index(drop=True)

    logger.info(
        f"Assembled {len(df)} rows of German solar data across {df['region'].nunique()} regions"
    )
    logger.debug(
        f"Timestamps go from {df['target_datetime_utc'].min()} "
        f"to {df['target_datetime_utc'].max()}"
    )

    return df


def fetch_de_data_forecast() -> pd.DataFrame:
    """
    Fetch the German national solar forecast via the ENTSO-E API.

    Which of the three ENTSO-E forecasts is collected is set by DE_FORECAST_TYPE.

    Returns DataFrame with 4 columns:
      - target_datetime_utc (UTC date and time)
      - solar_generation_kw (forecast generation in kilowatts)
      - region (data platform join key, "de")
      - forecast_type ("day_ahead", "intraday" or "current")
    """
    forecast_type = get_de_forecast_type()

    # ENTSO-E publishes the forecast for the day ahead, so we pull today and tomorrow
    start = pd.Timestamp.now(tz="UTC").floor("D")
    end = start + pd.Timedelta(days=2)

    logger.info(f"Fetching German {forecast_type} solar forecast from {start} to {end}")

    region = "de"
    forecast = _fetch_area_forecast(
        client=get_entsoe_client(),
        area=DE_AREAS[region],
        start=start,
        end=end,
        process_type=DE_FORECAST_PROCESS_TYPES[forecast_type],
    )

    if forecast.empty:
        logger.warning(f"No German {forecast_type} solar forecast data found")
        return pd.DataFrame(
            columns=[
                "target_datetime_utc",
                "solar_generation_kw",
                "region",
                "forecast_type",
            ]
        )

    forecast["region"] = region
    forecast["forecast_type"] = forecast_type
    forecast = forecast.sort_values("target_datetime_utc").reset_index(drop=True)

    logger.info(f"Assembled {len(forecast)} rows of German {forecast_type} solar forecast")
    logger.debug(
        f"Timestamps go from {forecast['target_datetime_utc'].min()} "
        f"to {forecast['target_datetime_utc'].max()}"
    )

    return forecast


def _fetch_area_generation(
    client: EntsoePandasClient, area: str, start: pd.Timestamp, end: pd.Timestamp
) -> pd.DataFrame:
    """Fetch solar generation for one ENTSO-E area.

    Returns a DataFrame with 'target_datetime_utc' and 'solar_generation_kw', which is empty
    if ENTSO-E has no data for this area and time window.
    """
    try:
        data = client.query_generation(area, start=start, end=end, psr_type=DE_SOLAR_PSR_TYPE)
    except NoMatchingDataError:
        logger.warning(f"No matching ENTSO-E data for {area} between {start} and {end}")
        data = None

    return _process_de_data(data, area=area, data_type="generation")


def _fetch_area_forecast(
    client: EntsoePandasClient,
    area: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    process_type: str,
) -> pd.DataFrame:
    """Fetch the solar forecast for one ENTSO-E area.

    Returns a DataFrame with 'target_datetime_utc' and 'solar_generation_kw', which is empty
    if ENTSO-E has no forecast for this area and time window.
    """
    try:
        data = client.query_wind_and_solar_forecast(
            area, start=start, end=end, psr_type=DE_SOLAR_PSR_TYPE, process_type=process_type
        )
    except NoMatchingDataError:
        logger.warning(f"No matching ENTSO-E forecast for {area} between {start} and {end}")
        data = None

    return _process_de_data(data, area=area, data_type="forecast")


def _process_de_data(data: pd.DataFrame | None, area: str, data_type: str) -> pd.DataFrame:
    """
    Common processing logic for both forecast and generation data.

    Parameters:
        data: Raw entsoe-py DataFrame, in the timezone of the area and in MW
        area: The ENTSO-E area the data is for
        data_type: Description for logging ("forecast" or "generation")
    """
    empty = pd.DataFrame(columns=["target_datetime_utc", "solar_generation_kw"])

    if data is None or data.empty:
        return empty

    solar = _get_solar_series(data)
    if solar is None:
        logger.warning(
            f"No solar column found in ENTSO-E {data_type} data for {area}, "
            f"got {list(data.columns)}"
        )
        return empty

    solar = solar.dropna()

    # ENTSO-E returns the values in the timezone of the area, and in MW
    return pd.DataFrame(
        {
            "target_datetime_utc": solar.index.tz_convert("UTC"),
            "solar_generation_kw": solar.values.astype(float) * 1000,
        }
    )


def _get_installed_capacity_kw(client: EntsoePandasClient, region: str, area: str) -> float:
    """Installed solar capacity (kW) for one area, from ENTSO-E.

    Returns NaN when capacity is unavailable, so saving can fall back to max generation.
    """
    capacity_mw = _fetch_installed_capacity_mw(client=client, area=area)
    if capacity_mw is None:
        logger.warning(f"No installed capacity from ENTSO-E for {region} ({area})")
        return float("nan")
    return capacity_mw * 1000


def _fetch_installed_capacity_mw(client: EntsoePandasClient, area: str) -> float | None:
    """Fetch the most recent ENTSO-E installed solar capacity (MW) for an area, or None."""
    end = pd.Timestamp.now(tz="Europe/Berlin")
    start = pd.Timestamp(year=end.year - 1, month=1, day=1, tz="Europe/Berlin")
    try:
        capacity = client.query_installed_generation_capacity(
            area, start=start, end=end, psr_type=DE_SOLAR_PSR_TYPE
        )
    except Exception as e:  # noqa: BLE001 - any failure returns None (save uses max generation)
        logger.warning(f"Could not fetch installed capacity for {area}: {e}")
        return None

    if capacity is None or len(capacity) == 0:
        return None

    series = capacity["Solar"] if "Solar" in capacity.columns else capacity.iloc[:, 0]
    series = series.dropna()
    if series.empty:
        return None

    # the most recent year available, whatever order ENTSO-E returns the rows in
    return float(series.loc[series.index.max()])


def _get_solar_series(data: pd.DataFrame) -> pd.Series | None:
    """Pull the solar generation column out of an entsoe-py generation DataFrame.

    Depending on what ENTSO-E returns, the columns are either flat ("Solar") or a MultiIndex
    of production type and either "Actual Aggregated" or "Actual Consumption".
    """
    if isinstance(data.columns, pd.MultiIndex):
        aggregated = [c for c in data.columns if c[-1] == "Actual Aggregated"]
        if aggregated:
            data = data[aggregated]
        data.columns = data.columns.get_level_values(0)

    if "Solar" not in data.columns:
        return None

    solar = data["Solar"]

    # a production type can appear more than once, in which case we get a DataFrame back
    if isinstance(solar, pd.DataFrame):
        solar = solar.iloc[:, 0]

    return solar

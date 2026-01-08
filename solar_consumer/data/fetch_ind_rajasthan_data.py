import datetime as dt
import logging
import time
from zoneinfo import ZoneInfo

import pandas as pd
import requests

log = logging.getLogger(__name__)

DEFAULT_DATA_URL = "http://sldc.rajasthan.gov.in/rrvpnl/read-sftp?type=overview"


def fetch_ind_rajasthan_data(data_url: str = DEFAULT_DATA_URL, retry_interval: int = 30, historic_or_forecast: str = "historic") -> pd.DataFrame:
    """
    Fetches the latest state-wide generation data for Rajasthan

    Args:
            data_url: The URL to query data from
            retry_interval: the amount of seconds to sleep between retying the api again.
            historic_or_forecast: It's not used but needed for the app to run

    Returns:
            A pandas DataFrame of generation values for wind and solar

    Raises:
            RuntimeError: If max retries are reached without any response
    """
    print("Starting to get data")
    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            r = requests.get(data_url, timeout=10)  # 10 second
            # Got a response (even if not 200), so break the retry loop
            break
        except requests.exceptions.Timeout as err:
            log.error("Timed out")
            retries += 1
            if retries == max_retries:
                error_msg = f"Failed to fetch data after {max_retries} attempts from {data_url}"
                log.error(error_msg)
                raise RuntimeError(error_msg) from err
            log.info(f"Retrying again in {retry_interval} seconds (retry count: {retries})")
            time.sleep(retry_interval)

    # Handle non-200 status codes by returning empty DataFrame
    if r.status_code != 200:
        log.warning(f"Failed to fetch data from {data_url}. Status code: {r.status_code}")
        return pd.DataFrame(columns=["energy_type", "target_datetime_utc", "solar_generation_kw"])

    raw_data = r.json()
    asset_map = {"WIND GEN": "wind", "SOLAR GEN": "solar"}
    data = []
    for k, v in asset_map.items():
        record = next((d["0"] for d in raw_data["data"] if d["0"]["scada_name"] == k), None)
        if record is not None:

            start_utc = dt.datetime.fromtimestamp(int(record["SourceTimeSec"]), tz=dt.UTC)
            power_kw = record["Average2"] * 1000  # source is in MW, convert to kW
            if power_kw < 0:
                log.warning(f"Ignoring negative power value: {power_kw} kW for asset type: {v}")
                continue
            if v == "wind":
                if start_utc < dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1):
                    start_ist = start_utc.astimezone(ZoneInfo("Asia/Kolkata"))
                    start_ist = str(start_ist)
                    now = dt.datetime.now(ZoneInfo("Asia/Kolkata"))
                    now = str(now)
                    timestamp_after_raise = f"Timestamp Now: {now} Timestamp data: {start_ist}"
                    timestamp_fstring = f"{timestamp_after_raise}"
                    log.warning("Start time is at least 1 hour old. " + timestamp_fstring)

            data.append({"energy_type": v, "target_datetime_utc": start_utc, "solar_generation_kw": power_kw})
            log.info(
                f"Found generation data for asset type: {v}, " f"{power_kw} kW at {start_utc} UTC"
            )
        else:
            log.warning(f"No generation data for asset type: {v}")

    return pd.DataFrame(data)

"""Script to download and process solar PV generation data for Belgium from the Elia Open Data API.

The data is downloaded in chunks using Dask for efficiency, and saved as a Zarr dataset.

This downloads all locations from Elia which includes the Belgium national total, the province 
estimates, and the Wallonia and Flanders regional totals which are sums of some provinces.
"""

import requests
import pandas as pd
import xarray as xr
from typing import Literal
import dask
from dask.diagnostics import ProgressBar


# USER CONFIGURABLE PARAMETERS

# Gather data between these dates (inclusive of start, exclusive of end)
start_datetime: pd.Timestamp = pd.Timestamp("2020-01-01T00:00:00+00:00")
end_datetime: pd.Timestamp = pd.Timestamp("2026-01-17T00:00:00+00:00")

# Set the time chunk size used when downloading and the scheduler and number of workers for dask
split_size: pd.Timedelta = pd.Timedelta("5D")
scheduler: Literal["threads", "processes"] = "threads"
num_workers: int = 10

# Set the save path and chunk size for the final dataset
save_chunk_scheme: dict[str, int] = {"time_utc": 1000, "location_name": -1}
save_path: str = "elia_belgium_pv_generation.zarr"


def fetch_elia_solar_data(
    start_datetime: pd.Timestamp, 
    end_datetime: pd.Timestamp, 
    source: Literal["historical", "live"] = "historical",
) -> xr.Dataset:
    """Fetch solar PV forecasting and estimation data from the Elia Open Data API.

    Args:
        start_datetime: The start datetime in ISO format (YYYY-MM-DDTHH:MM:SS).
        end_datetime: The end datetime in ISO format (YYYY-MM-DDTHH:MM:SS).
        source: The data source to download. "historical" for historical data, "live" for near 
            real-time.

    Returns:
        A xarray Dataset containing the solar data.
    """
    # Define the API Endpoint for "Solar PV Forecasting and Estimation"
    if source == "historical":
        # Dataset ID: ods032 (Historical)
        base_url = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods032/exports/json"
        generation_key = "measured"
    elif source == "live":
        # Dataset ID: ods087 (Near real-time)
        base_url = "https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods087/exports/json"
        generation_key = "realtime"

    params = {
        "select": f"datetime,region,{generation_key},monitoredcapacity",
        "where": f"datetime >= '{start_datetime}' AND datetime < '{end_datetime}'",
        "order_by": "datetime",
        "limit": -1,
        "timezone": "UTC",
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    if len(data) == 0:
        raise ValueError(f"No data returned for the period {start_datetime} to {end_datetime}.")

    df = pd.DataFrame(data)
    df = df.rename(
        columns={
            generation_key: "generation_mw", 
            "monitoredcapacity": "capacity_mwp",
            "region": "location_name",
            "datetime": "time_utc",
        }, 
    )
    df['time_utc'] = pd.DatetimeIndex(df["time_utc"]).tz_localize(None)
    return df.set_index(['time_utc', "location_name"]).to_xarray()


if __name__ == "__main__":

    # Create Dask tasks for each time chunk
    tasks = []
    for split_start_dt in pd.date_range(start=start_datetime, end=end_datetime, freq=split_size):
        split_end_dt = min(split_start_dt + split_size, end_datetime)
        if split_start_dt >= split_end_dt:
            continue
        tasks.append(dask.delayed(fetch_elia_solar_data)(split_start_dt, split_end_dt))
    
    # Compute the tasks with a progress bar
    with ProgressBar():
        dataset_chunks = dask.compute(*tasks, scheduler=scheduler, num_workers=num_workers)

    # Concatenate all dataset chunks along the time dimension and save as Zarr
    ds = (
        xr.concat(dataset_chunks, dim="time_utc")
        .sortby("time_utc")
        .astype("float32")
        .chunk(save_chunk_scheme)
    )

    ds.to_zarr(save_path, mode="w", consolidated=True)
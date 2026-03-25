"""This script downloads the GSP location data from NESO and the PVLive genereration data from
the PVLive API for all GSPs in Great Britain. 

The downloaded data is saved as a Zarr dataset.
"""

import fnmatch
import io
import logging
import os
import tempfile
import zipfile

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import xarray as xr
from ocf_data_sampler.select.geospatial import osgb_to_lon_lat
from pvlive_api import PVLive
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

# Set up a basic logger (if you don't already have one configured in your project)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ------------ USER CONFIGURABLE PARAMETERS ------------

# Set the date range to download
START_DT: pd.Timestamp = pd.Timestamp("2019-01-01 00:00")
END_DT: pd.Timestamp = pd.Timestamp("2025-12-31 23:30")

# Set the number of chunks to split the data into when downloading from the API
NUM_TIME_CHUNKS: int = 10

# Set the path to save the downloaded data to
SAVE_PATH: str = "pvlive_uk_pv_generation.zarr"

# Set the URL from which the GSP region boundaries zip can be downloaded. These change from time to 
# time so care is required
GSP_REGIONS_URL: str = (
    "https://api.neso.energy/dataset/2810092e-d4b2-472f-b955-d8bea01f9ec0/"
    "resource/c5647312-afab-4a58-8158-2f1efed1d7fc/download/gsp_regions_20251204.zip"
)

# File pattern within the downloaded region zip file which matches the GSP region boundaries 
# geojson file
GSP_REGIONS_GEOJSON_PATTERN_IN_ZIP: str = "Proj_27700/GSP_regions_27700_*.geojson"

# ------------ END OF USER CONFIGURABLE PARAMETERS ------------

# Set the URL from which the GSP name mapping can be downloaded. This is used to map from the GSP
# names in the region boundaries file to the GSP IDs used in the PVLive API.
GSP_NAME_MAP_URL = "https://api.pvlive.uk/pvlive/api/v4/gsp_list"

# Set up connection to PVLive
PVL_CONN = PVLive(domain_url="api.pvlive.uk")

if os.path.exists(SAVE_PATH):
    raise FileExistsError(
        f"The specified save path '{SAVE_PATH}' already exists. Please change "
        "the SAVE_PATH variable to avoid overwriting existing data."
    )


def get_gsp_boundaries() -> pd.DataFrame:
    """Get the GSP region boundaries"""

    with tempfile.TemporaryDirectory() as tmpdirname:

        # Download the GSP regions zip
        response_regions = requests.get(GSP_REGIONS_URL, timeout=30)
        response_regions.raise_for_status()

        # Unzip the downloaded GSP regions zip and find the relevant geojson file containing the 
        # GSP region boundaries
        with zipfile.ZipFile(io.BytesIO(response_regions.content)) as z:
            
            zip_file_names = z.namelist()        
            matched_files = fnmatch.filter(zip_file_names, GSP_REGIONS_GEOJSON_PATTERN_IN_ZIP)
            
            if not matched_files:
                raise FileNotFoundError(
                    f"Could not find a file matching '{GSP_REGIONS_GEOJSON_PATTERN_IN_ZIP}' "
                    "in the downloaded zip."
                )
            
            for matched_file in matched_files:
                z.extract(matched_file, tmpdirname)
            
            geojson_extract_path = os.path.join(tmpdirname, matched_files[0])
            
        # Load the GSP regions
        df_bound = gpd.read_file(geojson_extract_path)

        # Download the GSP name mapping - this maps name to GSP ID
        response_map = requests.get(GSP_NAME_MAP_URL, timeout=10)
        response_map.raise_for_status()

        # Load the GSP name mapping
        gsp_name_map = response_map.json()
        df_gsp_name_map = (
            pd.DataFrame(data=gsp_name_map["data"], columns=gsp_name_map["meta"])
            .drop("pes_id", axis=1)
        )

    # Some GSPs are split into multiple rows since they fall into different DNOs. We need to 
    # combine these rows to get the full GSP shape
    df_bound = (
        df_bound.groupby("GSPs")
        .apply(combine_gsps, include_groups=False)
        .reset_index()
    )

    # Add the PVLive GSP ID for each GSP
    df_bound = (
        df_bound.merge(df_gsp_name_map, left_on="GSPs", right_on="gsp_name")
        .drop("GSPs", axis=1)
    )

    # Add the national GSP - this is the union of all GSPs
    national_boundaries = gpd.GeoDataFrame(
        [["NATIONAL", df_bound.union_all(), 0]],
        columns=["gsp_name", "geometry", "gsp_id"],
        crs=df_bound.crs,
    )
    df_bound = pd.concat([national_boundaries, df_bound], ignore_index=True)

    # Add the coordinates for the centroid of each GSP
    df_bound["x_osgb"] = df_bound.geometry.centroid.x
    df_bound["y_osgb"] = df_bound.geometry.centroid.y

    # Convert the centroid coordinates from OSGB to lat/lon
    lons, lats = osgb_to_lon_lat(df_bound.x_osgb.values, df_bound.y_osgb.values)
    df_bound["longitude"] = lons
    df_bound["latitude"] = lats

    df_bound = df_bound.sort_values("gsp_id").reset_index(drop=True)

    return df_bound[["gsp_id", "gsp_name", "longitude", "latitude"]]


def combine_gsps(gdf: gpd.GeoDataFrame) -> gpd.GeoSeries:
    """Combine GSPs which have been split into mutliple rows."""
    return gpd.GeoSeries(gdf.union_all(), index=["geometry"], crs=gdf.crs)
    

# Use tenacity to retry the API call if it fails, with exponential backoff and logging
# This is important since the PVLive API can be unreliable at times, and we want to ensure we get
# all the data
@retry(
    reraise=True, 
    stop=stop_after_attempt(6), 
    wait=wait_exponential(multiplier=1, min=1, max=60),
    before_sleep=before_sleep_log(logger, logging.INFO)
)
def get_pvlive_gsp(
    gsp_id: int, 
    start_dt: pd.Timestamp, 
    end_dt: pd.Timestamp,
    num_chunks: int,
) -> pd.DataFrame:
    """Get the PVLive generation data for a given GSP ID.
    
    Args:
        gsp_id: The GSP ID to download data for.
        start_dt: The start datetime for the data to download.
        end_dt: The end datetime for the data to download.
        num_chunks: The number of chunks to split the data into when downloading from the API. This
            can help to avoid timeouts and memory issues when downloading large date ranges.
    """

    # Download the requested data in chunks
    time_chunk_bounds = pd.date_range(
        start_dt, end_dt + pd.Timedelta("30min"), 
        periods=num_chunks + 1
    ).ceil("30min")

    df_chunks = []
    for i in range(num_chunks):

        chunk_start = time_chunk_bounds[i]
        chunk_end = time_chunk_bounds[i + 1] - pd.Timedelta("30min")

        df_part = PVL_CONN.between(
            start=chunk_start.tz_localize("UTC"),
            end=chunk_end.tz_localize("UTC"),
            entity_type="gsp",
            entity_id=gsp_id,
            extra_fields="capacity_mwp",
            dataframe=True,
        )
        df_chunks.append(df_part)

    df = pd.concat(df_chunks).sort_values("datetime_gmt")

    # Remove the timezone
    df["datetime_gmt"] = df["datetime_gmt"].dt.tz_localize(None)

    if not df["datetime_gmt"].is_unique:
        raise ValueError(f"Duplicate datetimes found for GSP ID {gsp_id}")

    return df.set_index("datetime_gmt")



def get_all_pvlive_generation(
    start_dt: pd.Timestamp, 
    end_dt: pd.Timestamp, 
    gsp_ids: np.ndarray,
    longitudes: np.ndarray,
    latitudes: np.ndarray,
    num_chunks: int,
) -> xr.Dataset:
    """Get the PVLive generation data for all GSPs and return as an xarray Dataset.
    
    Args:
        start_dt: The start datetime for the data to download.
        end_dt: The end datetime for the data to download.
        gsp_ids: The GSP IDs to download data for.
        longitudes: The longitudes of the GSP centroids.
        latitudes: The latitudes of the GSP centroids.
        num_chunks: The number of chunks to split the data into when downloading from the API. This
            can help to avoid timeouts and memory issues when downloading large date ranges.
    """

    # Create empty array to store generation data
    target_times = pd.date_range(start_dt, end_dt, freq="30min")

    data_shape = (len(target_times), len(gsp_ids))

    ds_generation = xr.Dataset(
        {
            "generation_mw": (("time_utc", "location_id"), np.zeros(data_shape)),
            "capacity_mwp": (("time_utc", "location_id"), np.zeros(data_shape)),
        },
            coords={
                "time_utc": target_times,
                "location_id": gsp_ids,
                "longitude": (("location_id",), longitudes),
                "latitude": (("location_id",), latitudes),
            }
    )
    for gsp_id in tqdm(ds_generation.location_id.values):
        df = get_pvlive_gsp(gsp_id, start_dt, end_dt, num_chunks=num_chunks)
        
        # Check the expected times are present
        if not (df.index == ds_generation.time_utc).all():
            raise ValueError(f"Expected times do not match for GSP ID {gsp_id}")
        
        # Store the values in the Dataset
        ds_generation.capacity_mwp.loc[{"location_id": gsp_id}] = df.capacity_mwp.values
        ds_generation.generation_mw.loc[{"location_id": gsp_id}] = df.generation_mw.values
    
    return ds_generation


def main() -> None:

    # Get the GSP region boundaries
    df_bound = get_gsp_boundaries()

    # Check the GSP IDs pulled from the url match the PVLive API
    if not df_bound[["gsp_id", "gsp_name"]].equals(PVL_CONN.gsp_list[["gsp_id", "gsp_name"]]):
        raise ValueError("GSP IDs pulled from url do not match PVLive API")
    
    # Get the generation data for all GSPs and save
    ds_generation = get_all_pvlive_generation(
        start_dt=START_DT, 
        end_dt=END_DT, 
        gsp_ids=df_bound["gsp_id"].values,
        longitudes=df_bound["longitude"].values,
        latitudes=df_bound["latitude"].values,
        num_chunks=NUM_TIME_CHUNKS,
    )

    ds_generation.to_zarr(SAVE_PATH, mode="w-", consolidated=False)


if __name__ == "__main__":
    main()
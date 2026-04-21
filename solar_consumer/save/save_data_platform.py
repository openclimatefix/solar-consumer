"""Functions to save to the Data-platform

https://github.com/openclimatefix/data-platform

"""

import datetime
from ocf import dp
import pandas as pd

import asyncio
import logging
from collections import defaultdict

import itertools

import betterproto

from betterproto.lib.google.protobuf import Struct, Value
from pathlib import Path
from importlib.metadata import version


def _get_country_config(country: str) -> dict:
    """Get country-specific configuration for data platform operations."""
    configs = {
        "nl": {
            "id_key": "region_id",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "number",  
            "observer_name": "nednl",
        },
        "be": {
            "id_key": "region",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "string",  
            "observer_name": "elia_be",
        },
        "gb": {
            "required_observers": {"pvlive_in_day", "pvlive_day_after"},
            "id_key": "gsp_id",
            "location_type": [dp.LocationType.GSP, dp.LocationType.NATION],
            "metadata_type": "number", 
            "observer_name": None, 
        },
        "ind_rajasthan": {
            "id_key": "name",
            "location_type": [dp.LocationType.STATE],
            "metadata_type": "string",
            "observer_name": "ruvnl",
        },
    }
    return configs.get(country, configs["gb"])


def _extract_metadata_value(metadata: dict, key: str, metadata_type: str) -> any:
    """Extract value from location metadata based on type."""
    if metadata_type == "number":
        return metadata.get(key, {}).get("number_value")
    else:  # string
        return metadata.get(key, {}).get("string_value")


async def _execute_async_tasks(
    tasks: list[asyncio.Task],
    ignore_exceptions: bool = False,
) -> list[any]:
    """Execute a list of tasks and check for exceptions."""
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    if not ignore_exceptions:
        for exc in filter(lambda x: isinstance(x, Exception), results):
            raise exc
    return results


async def _list_locations(
    client: dp.DataPlatformDataServiceStub,
    location_type: dp.LocationType | list[dp.LocationType],
    country: str = "gb",
) -> list[dict]:
    """List locations from data platform and convert to dict format."""
    if country == "ind_rajasthan":
        es_filter = dp.EnergySource.UNSPECIFIED
    else:
        es_filter = dp.EnergySource.SOLAR

    if isinstance(location_type, list):
        # Handle multiple location types (e.g., GB with GSP and NATION)
        tasks = [
            asyncio.create_task(
                client.list_locations(
                    dp.ListLocationsRequest(
                        location_type_filter=loc_type,
                        energy_source_filter=es_filter,
                    )
                )
            )
            for loc_type in location_type
        ]
        list_results = await _execute_async_tasks(tasks)        
        all_locations = list(
            itertools.chain(
                *[
                    r.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True)[
                        "locations"
                    ]
                    for r in list_results
                ]
            )
        )
    else:
        # Single location type
        list_locations_request = dp.ListLocationsRequest(
            location_type_filter=location_type,
            energy_source_filter=es_filter,
        )
        list_locations_response = await client.list_locations(list_locations_request)
        all_locations = list_locations_response.to_dict(
            casing=betterproto.Casing.SNAKE,
            include_default_values=True,
        ).get("locations", [])

    # Filter based on country metadata
    filtered_locations = []
    for loc in all_locations:
        metadata = loc.get("metadata", {})
        # Extract metadata value, struct usually converts to dictionary
        # metadata structure: {'key': 'value'} or {'key': {'string_value': 'value'}}    
        
        val_dict = metadata.get("country", {})
        loc_country = val_dict.get("string_value")

        # make sure effective_capacity_watts is a float
        loc["effective_capacity_watts"] = float(loc["effective_capacity_watts"])

        if country == "gb":
            # For GB, assume it matches if country is "gb" OR if country metadata is missing.
            # This ensures backward compatibility for existing GB locations.
            if loc_country == "gb" or not loc_country:
                filtered_locations.append(loc)
        else:
            # For NL/BE, strict matching
            if loc_country == country:
                filtered_locations.append(loc)

    return filtered_locations


async def _create_locations_from_csv(
    client: dp.DataPlatformDataServiceStub,
    country: str,
    id_key: str,
    metadata_type: str,
) -> None:
    """Create locations from CSV file for countries that support it (NL, BE)."""
    csv_path = Path(__file__).parent.parent / "data" / "locations.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Unified locations CSV not found at {csv_path}")
    
    locations_df_csv = pd.read_csv(csv_path)
    # Filter by country code
    locations_df_csv = locations_df_csv[locations_df_csv['country_code'] == country]
    locations = locations_df_csv.to_dict(orient="records")
    
    for location in locations:
        location_name = location["name"]
        location_type_str = location.get("location_type", "NATION")

        effective_capacity_watts = 100_000_000_000
        
        # Create metadata based on type (number or string)
        id_value = location[id_key]
        metadata_fields = {
            "country": Value(string_value=country),
        }
        if metadata_type == "number":
            metadata_fields[id_key] = Value(number_value=id_value)
        else:  # string
            metadata_fields[id_key] = Value(string_value=id_value)

        metadata = Struct(fields=metadata_fields)
        
        if location_type_str == "NATION":
             location_type = dp.LocationType.NATION
        elif location_type_str == "STATE":
             location_type = dp.LocationType.STATE
        else:
             location_type = dp.LocationType.NATION

        energy_source_str = location.get("energy_source", "solar").lower()
        if energy_source_str == "wind":
            energy_source = dp.EnergySource.WIND
        else:
            energy_source = dp.EnergySource.SOLAR
        create_location_request = dp.CreateLocationRequest(
            location_name=location_name,
            energy_source=energy_source,
            location_type=location_type,
            geometry_wkt=f"POINT({location['longitude']} {location['latitude']})",
            effective_capacity_watts=effective_capacity_watts,
            metadata=metadata,
            valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        )
        await client.create_location(create_location_request)
    
    logging.warning(
        f"No {country.upper()} locations found in data platform. Created new locations."
    )


async def save_generation_to_data_platform(
    data_df: pd.DataFrame, client: dp.DataPlatformDataServiceStub, country: str = "gb"
) -> None:
    """
    Saves model data via the data platform.

    Incoming data is enriched with location information from the data platform. Anything with zero
    capacity, or without a corresponding entry in the data platform, is ignored.

    For GB: Data is joined via the gsp_id, which is a column in the incoming data, and has to be
    extracted from the metadata field in the data platform location data.

    For NL: Data is joined via the region_id.

    For BE: Data is joined via the region (string-based matching).

    Args:
        data_df: DataFrame containing the generation data
        client: Data platform client stub
        country: Country identifier ('gb', 'nl', or 'be')
    """
    tasks: list[asyncio.Task] = []
    config = _get_country_config(country)
    
    id_key = config["id_key"]
    # capacity_col and capacity_multiplier are no longer needed as we standardized on capacity_kw
    metadata_type = config["metadata_type"]
    
    # Determine required observers
    # If observer_name is in config (NL/BE), use it as the single required observer
    # If not (GB), use the explicit list from config
    observer_name_config = config["observer_name"]
    if observer_name_config:
        required_observers = {observer_name_config}
    else:
        required_observers = config["required_observers"]

    # 0. Create the observers required if they don't exist already

    list_observer_request = dp.ListObserversRequest(
        observer_names_filter=list(required_observers),
    )
    list_observer_response = await client.list_observers(list_observer_request)
    create_observers = required_observers.difference(
        {observer.observer_name for observer in list_observer_response.observers}
    )
    for observer_name in create_observers:
        tasks.append(
            asyncio.create_task(
                client.create_observer(dp.CreateObserverRequest(name=observer_name))
            )
        )
    if len(tasks) > 0:
        logging.info("creating %d observers", len(tasks))
        await _execute_async_tasks(tasks)

    # 1. Get locations and join to the incoming data.
    if country in ["nl", "be", "ind_rajasthan"]:
        # NL and BE support CSV-based location creation
        locations_data = await _list_locations(client, config["location_type"], country=country)
        
        if not locations_data:
            await _create_locations_from_csv(client, country, id_key, metadata_type)
            # Re-fetch locations after creating them
            locations_data = await _list_locations(client, config["location_type"], country=country)
    else:
        # GB - no CSV creation support
        locations_data = await _list_locations(client, config["location_type"], country=country)

    # Convert locations to DataFrame
    locations_df = pd.DataFrame.from_dict(locations_data)

    # Prepare incoming data copy
    data_df = data_df.copy()

    has_capacity_data = "capacity_kw" in data_df.columns
    if not has_capacity_data:
        data_df["capacity_kw"] = data_df["solar_generation_kw"].max()
        logging.info("No capacity info found, so using max generation")

    if country == "ind_rajasthan":
        data_df["name"] = "ruvnl_" + data_df["energy_type"].astype(str)
    
    # Extract metadata and create join key based on country
    if country == "be":
        # BE uses string matching with normalization
        data_df["join_key"] = data_df[id_key]
        
        if locations_df.empty or data_df.empty:
            joined_df = pd.DataFrame()
        else:
            locations_df = locations_df.assign(
                join_key=lambda df: df["metadata"].apply(
                    lambda x: _extract_metadata_value(x, id_key, metadata_type)
                )
            ).assign(
                join_key=lambda df: df["join_key"].fillna(df["location_name"])
            ).assign(
                join_key=lambda df: df["join_key"].astype(str).str.strip().str.lower()
            )
    else:
        # NL and GB use numeric matching
        data_df["join_key"] = data_df[id_key]
        
        locations_df = (
            locations_df
            .loc[lambda df: df["metadata"].apply(lambda x: id_key in x)]
            .assign(
                join_key=lambda df: df["metadata"].apply(
                    lambda x: _extract_metadata_value(x, id_key, metadata_type)
                )
            )
        )
    
    # Common join logic for all countries
    if not (locations_df.empty or data_df.empty):
        joined_df = (
            locations_df
            .set_index("join_key")
            .join(
                data_df.query("capacity_kw!=0").set_index("join_key"),
                on="join_key",
                how="inner",
                lsuffix="_loc",
            )
            .assign(
                new_effective_capacity_watts=lambda df: (
                    df["capacity_kw"] * 1000
                )
            )
            .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
        )
    else:
        joined_df = pd.DataFrame()

    if joined_df.empty:
        # Check if the input data was empty or had no valid capacity data
        has_valid_capacity_data = not data_df.empty and (data_df["capacity_kw"] != 0).any()
        
        if data_df.empty or not has_valid_capacity_data:
            # Empty input or all zero-capacity data - this is expected, return silently
            return
        
        # Non-empty data with capacity but no matching locations - this is unexpected
        incoming_ids = data_df[id_key].unique().tolist() if id_key in data_df.columns else []
        raise ValueError(
            f"No matching {country.upper()} locations found for the incoming data. "
            f"Expected locations to exist in the data platform with {id_key} metadata "
            f"matching the following {id_key} values: {incoming_ids}. "
            f"This is unexpected - locations should have been created or already exist."
        )

    logging.info(
        "handling %s data for %d matched locations",
        country.upper(),
        joined_df["location_uuid"].nunique(),
    )

    # 2. Generate the UpdateLocationCapacityRequest objects from the DataFrame.
    # * Should only occur when the incoming data has a different capacity to that returned by the
    # * data platform. The most recent value for a given location is the one that is used.
    if has_capacity_data:
        updates_df = get_update_capacity_df(joined_df)

        tasks = []
        for row in updates_df.itertuples():
            lid = row.location_uuid
            t = row.target_datetime_utc
            new_cap = row.new_effective_capacity_watts
            metadata = row.metadata

            # this is specific to GB consumer at the moment
            if "capacity_no_degradation_kw" in updates_df.columns:
                metadata = format_metadata_from_dict(metadata=row.metadata)
                metadata["capacity_no_degradation_kw"] = Value(number_value=int(row.capacity_no_degradation_kw))
                metadata = Struct(fields=metadata)
            else:
                metadata = None

            req = dp.UpdateLocationRequest(
                location_uuid=lid,
                energy_source=dp.EnergySource.SOLAR,
                new_effective_capacity_watts=int(new_cap),
                valid_from_utc=t,
                new_metadata=metadata,
            )
            tasks.append(asyncio.create_task(client.update_location(req)))

        if len(tasks) > 0:
            logging.info("updating %d %s location capacities", len(tasks), country.upper())
            await _execute_async_tasks(tasks, ignore_exceptions=False)

    # 3. Generate the CreateObservationRequest objects from the DataFrame.

    # lets check none of the values are above 109% of the capacity
    # the limit is 110% but sometimes there are some rounding errors
    # if they are lets remove them
    if has_capacity_data:
      idx = joined_df["solar_generation_kw"] > (joined_df["capacity_kw"] * 1.09)
      if idx.any():
          location_uuids = joined_df.loc[idx, "location_uuid"].unique()
          logging.warning(f"Found {idx.sum()} values above 109% of capacity \
                          for location_uuid {location_uuids}. \
                          These values will be dropped.")
          joined_df = joined_df[~idx]


    observations_by_loc: dict[str, list[dp.CreateObservationsRequestValue]] = defaultdict(list)
    energy_source_by_loc: dict[str, dp.EnergySource] = {}
    for lid, t, val, es in zip(
        joined_df["location_uuid"],
        joined_df["target_datetime_utc"],
        (joined_df["solar_generation_kw"] * 1000).astype(int),
        joined_df["energy_source"],
    ):
        observations_by_loc[lid].append(
            dp.CreateObservationsRequestValue(timestamp_utc=t, value_watts=val)
        )
        energy_source_by_loc[lid] = dp.EnergySource[es]

    # Determine observer name based on country
    observer_name = config["observer_name"]
    if observer_name is None:  # GB needs regime from data
        regime: str = data_df["regime"].values[0]
        observer_name = f"pvlive_{regime.replace('-', '_')}"

    tasks = [
        asyncio.create_task(
            client.create_observations(
                dp.CreateObservationsRequest(
                    location_uuid=lid,
                    energy_source=energy_source_by_loc[lid],
                    observer_name=observer_name,
                    values=vals,
                ),
            )
        )
        for lid, vals in observations_by_loc.items()
    ]

    if len(tasks) > 0:
        logging.info("creating observations for %d %s locations", len(tasks), country.upper())
        await _execute_async_tasks(tasks)

async def save_forecasts_to_data_platform(
    data_df: pd.DataFrame, 
    client: dp.DataPlatformDataServiceStub,
    model_tag: str, 
    model_version: str, 
    init_time_utc: datetime,
    country: str = "gb",
) -> None:
    """
    Save NESO solar forecast data to the data platform.

    The NESO forecast is national-level (gsp_id=0) and contains 3 types of forecasts
    that get updated at different times.

    Args:
        data_df: DataFrame with columns 'target_datetime_utc' and 'solar_generation_kw'
        client: Data platform client stub
        model_tag: Model tag identifier for the forecast
        model_version: Model version string
        country: Country code for the forecast data.
    """
    # 1. Ensure the forecaster exists
    forecaster = await create_forecaster_if_not_exists(client, model_tag, model_version)
    logging.info("Using forecaster: %s (version: %s)", forecaster.forecaster_name, forecaster.forecaster_version)
    
    # 2. Get the national location (gsp_id=0)
    locations_data = await _list_locations(
        client,
        location_type=[dp.LocationType.NATION],
        country=country,
    )
    
    # Find the national location
    national_location = None
    
    for loc in locations_data:
        metadata = loc.get("metadata", {})
        if country == "gb":
            # GB specific: look for gsp_id=0
            gsp_id = metadata.get("gsp_id", {}).get("number_value")
            if gsp_id == 0:
                national_location = loc
                break
        else:
            national_location = loc
            break
    
    if national_location is None:
        raise ValueError(
            f"No national location found in data platform for {country.upper()}. "
            "Please ensure it exists before saving forecasts."
        )
    
    location_uuid = national_location["location_uuid"]
    effective_capacity_watts = float(national_location.get("effective_capacity_watts", 0))
    
    if effective_capacity_watts <= 0:
        raise ValueError(
            f"National location (gsp_id=0) has invalid effective_capacity_watts: {effective_capacity_watts}. "
            "Cannot calculate forecast fractions."
        )

    # 3. Determine init_time_utc and horizon mins
    init_time_utc = init_time_utc.replace(tzinfo=None)
    # create horizon mins
    target_datetime_utc = pd.to_datetime(data_df['target_datetime_utc'].values)
    horizons_mins = (target_datetime_utc - init_time_utc).total_seconds() / 60
    horizons_mins = horizons_mins.astype(int)

    # Get p50 fractions
    p50s = data_df["solar_generation_kw"].values.astype(float)
    p50s = p50s * 1000 / float(effective_capacity_watts)  # kW -> W, then fraction
    
    forecast_values = []
    
    for h, p50 in zip(horizons_mins, p50s, strict=True):
        if h < 0:
            # Skip targets in the past relative to init time
            continue
        forecast_values.append(
            dp.CreateForecastRequestForecastValue(
                horizon_mins=h,
                p50_fraction=p50,
                metadata=Struct().from_pydict({}),
                other_statistics_fractions={},
            )
        )    
    
    if not forecast_values:
        logging.warning("No valid forecast values to save.")
        return
    
    # 4. Save forecast using the forecaster
    await client.create_forecast(
        dp.CreateForecastRequest(
            forecaster=forecaster,
            location_uuid=location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            init_time_utc=init_time_utc.replace(tzinfo=datetime.timezone.utc),
            values=forecast_values,
        )
    )
    
    logging.info(
        "Saved %d NESO forecast values to data platform for location %s (init_time: %s)",
        len(forecast_values),
        location_uuid,
        init_time_utc.isoformat(),
    )

async def create_forecaster_if_not_exists(
    client: dp.DataPlatformDataServiceStub,
    model_tag: str,
    model_version: str | None = None,
) -> dp.Forecaster:
    """Create the current forecaster if it does not exist.
    
    Args:
        client: Data platform client stub
        model_tag: Model tag identifier (will be converted to forecaster name)
        model_version: Version string for the forecaster. If None, uses package version.
    
    Returns:
        The Forecaster object (existing or newly created/updated)
    """
    name = model_tag.replace("-", "_")
    
    if model_version is None:
        app_version = version("neso-solar-consumer")
    else:
        app_version = model_version

    list_forecasters_request = dp.ListForecastersRequest(
        forecaster_names_filter=[name],
    )
    list_forecasters_response = await client.list_forecasters(list_forecasters_request)

    if len(list_forecasters_response.forecasters) > 0:
        filtered_forecasters = [
            f for f in list_forecasters_response.forecasters if f.forecaster_version == app_version
        ]
        if len(filtered_forecasters) == 1:
            # Forecaster exists, return it
            return filtered_forecasters[0]
        else:
            # Forecaster version does not exist, update it
            update_forecaster_request = dp.UpdateForecasterRequest(
                name=name,
                new_version=app_version,
            )
            update_forecaster_response = await client.update_forecaster(update_forecaster_request)
            return update_forecaster_response.forecaster
    else:
        # Forecaster does not exist, create it
        create_forecaster_request = dp.CreateForecasterRequest(
            name=name,
            version=app_version,
        )
        create_forecaster_response = await client.create_forecaster(create_forecaster_request)
        return create_forecaster_response.forecaster

def format_metadata_from_dict(metadata):
    """ Format the dict keys and values to the expected format """
    for k,v in metadata.items():
        if isinstance(v, Value):
            continue
        elif isinstance(v, dict) and v["string_value"] != '':
            metadata[k] = Value(string_value=v["string_value"])
        else:
            metadata[k] = Value(number_value=v["number_value"])
    return metadata


def get_update_capacity_df(df: pd.DataFrame) -> pd.DataFrame:
    """Get the rows that need to be updated based on capacity change."""

    # lets only consider non nans values
    df = df[~df["new_effective_capacity_watts"].isna()]

    if "update_capacity" in df.columns:
        # only update capacity if this is set to True
        # we use this in NL for non-validated capacities
        df = df[df['update_capacity']]

    # lets make sure we use the latest timestamp for each location_uuid
    df = df.sort_values(by="target_datetime_utc", ascending=False).groupby("location_uuid").head(1)

    current_cap = df["effective_capacity_watts"]
    new_cap = df["new_effective_capacity_watts"]

    # only update if the difference is more than one
    update_idx = (current_cap - new_cap).abs() >= 1

    updates_df = (
        df.loc[update_idx]
        .sort_values(by="target_datetime_utc", ascending=False)
        .groupby(level=0)
        .head(1)
        .sort_index()
    )
    return updates_df
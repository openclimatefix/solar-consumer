"""Functions to save to the Data-platform

https://github.com/openclimatefix/data-platform

"""

import datetime
from dp_sdk.ocf import dp
import pandas as pd

import asyncio
import logging
from collections import defaultdict

import itertools

import betterproto

import numpy as np
from betterproto.lib.google.protobuf import Struct, Value
from pathlib import Path


def _get_country_config(country: str) -> dict:
    """Get country-specific configuration for data platform operations."""
    configs = {
        "nl": {
            "id_key": "region_id",
            "location_type": [dp.LocationType.NATION, dp.LocationType.COUNTY],
            "metadata_type": "number",  
            "observer_name": "nednl",
        },
        "be": {
            "id_key": "region",
            "location_type": [dp.LocationType.NATION, dp.LocationType.COUNTY],
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
    if isinstance(location_type, list):
        # Handle multiple location types (e.g., GB with GSP and NATION)
        tasks = [
            asyncio.create_task(
                client.list_locations(
                    dp.ListLocationsRequest(
                        location_type_filter=loc_type,
                        energy_source_filter=dp.EnergySource.SOLAR,
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
            energy_source_filter=dp.EnergySource.SOLAR,
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
        elif location_type_str == "COUNTY":
             location_type = dp.LocationType.COUNTY
        else:
             location_type = dp.LocationType.NATION

        create_location_request = dp.CreateLocationRequest(
            location_name=location_name,
            energy_source=dp.EnergySource.SOLAR,
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
    if country in ["nl", "be"]:
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
                data_df.query("capacity_kw>0").set_index("join_key"),
                on="join_key",
                how="inner",
                lsuffix="_loc",
            )
            .assign(
                new_effective_capacity_watts=lambda df: (
                    df["capacity_kw"] * 1000
                ).astype(int)
            )
            .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
        )
    else:
        joined_df = pd.DataFrame()

    if joined_df.empty:
        # Check if the input data was empty or had no valid capacity data
        has_valid_capacity_data = not data_df.empty and (data_df["capacity_kw"] > 0).any()
        
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
    #
    # TODO, we've put in a limit of relative tolerance of 2% here to avoid tiny changes triggering updates,
    # This is references in https://github.com/openclimatefix/data-platform/issues/71
    joined_df["capacity_change"] = (
        (joined_df["effective_capacity_watts"].astype(float))
        / (joined_df["new_effective_capacity_watts"].astype(float))
    ).abs()

    logging.info(f"DEBUG: variables joined_df size: {len(joined_df)}")
    if not joined_df.empty:
        logging.info(f"DEBUG: joined_df sample:\n{joined_df[['location_name', 'effective_capacity_watts', 'new_effective_capacity_watts', 'capacity_change']].head()}")

    updates_df = (
        joined_df.loc[lambda df: ~np.isclose(df["capacity_change"], 1.0, rtol=0.02)]
        .sort_values(by="target_datetime_utc", ascending=False)
        .groupby(level=0)
        .head(1)
        .sort_index()
    )

    logging.info(f"DEBUG: updates_df size: {len(updates_df)}")
    if not updates_df.empty:
        logging.info(f"DEBUG: updates_df sample:\n{updates_df[['location_name', 'effective_capacity_watts', 'new_effective_capacity_watts']].head()}")

    tasks = []
    for lid, t, new_cap in zip(
        updates_df["location_uuid"],
        updates_df["target_datetime_utc"],
        updates_df["new_effective_capacity_watts"],
    ):
        logging.info(f"DEBUG: Creating update task for {lid} with new_cap={new_cap}, valid_from={t}")
        req = dp.UpdateLocationRequest(
            location_uuid=lid,
            energy_source=dp.EnergySource.SOLAR,
            new_effective_capacity_watts=new_cap,
            valid_from_utc=t,
        )
        # TODO if we run the next line, the tests seem to pass
        # _ = await client.update_location(req)
        ######## code that doesnt work (maybe)
        tasks.append(asyncio.create_task(client.update_location(req)))

    if len(tasks) > 0:
        logging.info("updating %d %s location capacities", len(tasks), country.upper())
        # NL was previously ignoring these exceptions
        await _execute_async_tasks(tasks, ignore_exceptions=False)

    ######## code that doesnt work (maybe)

    # 3. Generate the CreateObservationRequest objects from the DataFrame.
    observations_by_loc: dict[str, list[dp.CreateObservationsRequestValue]] = defaultdict(list)
    for lid, t, val in zip(
        joined_df["location_uuid"],
        joined_df["target_datetime_utc"],
        (joined_df["solar_generation_kw"] * 1000).astype(int),
    ):
        observations_by_loc[lid].append(
            dp.CreateObservationsRequestValue(timestamp_utc=t, value_watts=val)
        )

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
                    energy_source=dp.EnergySource.SOLAR,
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

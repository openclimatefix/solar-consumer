"""Shared helpers for saving to the Data-platform

https://github.com/openclimatefix/data-platform

"""

import asyncio
import datetime
import itertools
from pathlib import Path

import betterproto
import pandas as pd
from betterproto.lib.google.protobuf import Struct, Value
from loguru import logger
from ocf import dp


def _get_country_config(country: str) -> dict:
    """Get country-specific configuration for data platform operations."""
    configs = {
        "nl": {
            "id_key": "region_id",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "number",  
            "observer_name": "nednl",
            "country": "nl"
        },
        "nl_no_curtailment": {
            "id_key": "region_id",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "number",  
            "observer_name": "nednl_no_curtailment",
            "country": "nl"
        },
        "be": {
            "id_key": "region",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "string",  
            "observer_name": "elia_be",
            "country": "be"
        },
        "de": {
            "id_key": "region",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "string",
            "observer_name": "entsoe_de",
            "country": "de",
            "rolling_capacity": True
        },
        "gb": {
            "required_observers": {"pvlive_in_day", "pvlive_day_after"},
            "id_key": "gsp_id",
            "location_type": [dp.LocationType.GSP, dp.LocationType.NATION],
            "metadata_type": "number", 
            "observer_name": None, 
            "country": "gb"
        },
        "ind_rajasthan": {
            "id_key": "name",
            "location_type": [dp.LocationType.STATE],
            "metadata_type": "string",
            "observer_name": "ruvnl",
            "country": "ind_rajasthan"
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
    for exc in filter(lambda x: isinstance(x, Exception), results):
        if not ignore_exceptions:
            raise exc
        else:
            logger.warning(f"Task failed: {exc}")
    return results


async def _list_locations(
    client: dp.DataPlatformDataServiceStub,
    location_type: list[dp.LocationType],
    country: str = "gb",
) -> list[dict]:
    """List locations from data platform and convert to dict format."""
    if country == "ind_rajasthan":
        es_filters = [dp.EnergySource.SOLAR, dp.EnergySource.WIND]
    else:
        es_filters = [dp.EnergySource.SOLAR]

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
        for es_filter in es_filters
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
            # For NL/BE/DE, strict matching
            if loc_country == country:
                filtered_locations.append(loc)

    return filtered_locations


async def _create_locations_from_csv(
    client: dp.DataPlatformDataServiceStub,
    country: str,
    id_key: str,
    metadata_type: str,
    capacity_watts_by_id: dict | None = None,
) -> None:
    """Create locations from CSV file for countries that support it (NL, BE, DE).

    ``capacity_watts_by_id`` maps a join-key value to its capacity in watts, taken from the
    incoming data, so a new location starts with a realistic capacity. Anything not present
    falls back to 100 MW, which later gets updated from the incoming data.
    """
    capacity_watts_by_id = capacity_watts_by_id or {}

    csv_path = Path(__file__).parent.parent.parent / "data" / "locations.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Unified locations CSV not found at {csv_path}")
    
    locations_df_csv = pd.read_csv(csv_path)
    # Filter by country code
    locations_df_csv = locations_df_csv[locations_df_csv['country_code'] == country]
    locations = locations_df_csv.to_dict(orient="records")

    created_uuids: dict[str, str] = {}
    for location in locations:
        location_name = location["name"]
        location_type_str = location.get("location_type", "NATION")
        
        # Create metadata based on type (number or string)
        id_value = location[id_key]

        effective_capacity_watts = int(capacity_watts_by_id.get(id_value, 100_000_000))

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

        # locations listed more than once with different energy sources, so we create one location per energy source
        if location_name in created_uuids:
            await client.create_location_energy_source(
                dp.CreateLocationEnergySourceRequest(
                    location_uuid=created_uuids[location_name],
                    energy_source=energy_source,
                    effective_capacity_watts=effective_capacity_watts,
                    metadata=metadata,
                    valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
                )
            )
            continue

        create_location_request = dp.CreateLocationRequest(
            location_name=location_name,
            energy_source=energy_source,
            location_type=location_type,
            geometry_wkt=f"POINT({location['longitude']} {location['latitude']})",
            effective_capacity_watts=effective_capacity_watts,
            metadata=metadata,
            valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC),
        )
        create_location_response = await client.create_location(create_location_request)
        created_uuids[location_name] = create_location_response.location_uuid
    
    logger.warning(
        f"No {country.upper()} locations found in data platform. Created new locations."
    )

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

"""Shared helpers for saving to the Data-platform

https://github.com/openclimatefix/data-platform

"""

import asyncio
import itertools

import betterproto
from betterproto.lib.google.protobuf import Value
from loguru import logger
from ocf import dp


def point_to_polygon_wkt(longitude: float, latitude: float, size: float = 0.01) -> str:
    """A small square polygon around a point, as the data platform only allows POINTs for sites."""
    west, east = longitude - size, longitude + size
    south, north = latitude - size, latitude + size
    return (
        f"POLYGON(({west} {south}, {east} {south}, {east} {north}, "
        f"{west} {north}, {west} {south}))"
    )


def _get_country_config(country: str) -> dict:
    """Get country-specific configuration for data platform operations."""
    configs = {
        "nl": {
            "id_key": "region_id",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "number",  
            "observer_name": "nednl",
            "country": "nl",
            "country_code": "NL",
        },
        "nl_no_curtailment": {
            "id_key": "region_id",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "number",  
            "observer_name": "nednl_no_curtailment",
            "country": "nl",
            "country_code": "NL",
        },
        "be": {
            "id_key": "region",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "string",  
            "observer_name": "elia_be",
            "country": "be",
            "country_code": "BE",
        },
        "de": {
            "id_key": "region",
            "location_type": [dp.LocationType.NATION, dp.LocationType.STATE],
            "metadata_type": "string",
            "observer_name": "entsoe_de",
            "country": "de",
            "country_code": "DE",
            "rolling_capacity": True
        },
        "gb": {
            "required_observers": {"pvlive_in_day", "pvlive_day_after"},
            "id_key": "gsp_id",
            "location_type": [dp.LocationType.GSP, dp.LocationType.NATION],
            "metadata_type": "number", 
            "observer_name": None, 
            "country": "gb",
            "country_code": "GB",
        },
        "ind_rajasthan": {
            "id_key": "name",
            "location_type": [dp.LocationType.STATE],
            "metadata_type": "string",
            "observer_name": "ruvnl",
            "country": "ind_rajasthan",
            "country_code": "IN",
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

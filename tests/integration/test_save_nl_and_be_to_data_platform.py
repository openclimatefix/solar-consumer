import pandas as pd
import pytest
import datetime
from betterproto.lib.google.protobuf import Struct, Value
import betterproto

from solar_consumer.save.save_data_platform import save_generation_to_data_platform

from dp_sdk.ocf import dp


# Country-specific configuration for parametrized tests
NL_CONFIG = {
    "country": "nl",
    "observer_name": "nednl",
    "locations": [
        {
            "name": "nl_national",
            "metadata_key": "region_id",
            "metadata_value": 0,
            "metadata_type": "number",
            "geometry": "POINT(5.29 52.13)",
            "capacity": 100_000_000_000,
        },
        {
            "name": "nl_groningen",
            "metadata_key": "region_id",
            "metadata_value": 1,
            "metadata_type": "number",
            "geometry": "POINT(6.74 53.22)",
            "capacity": 50_000_000_000,
        },
    ],
    "test_data": {
        "target_datetime_utc": [
            pd.to_datetime("2025-01-01T00:00:00Z"),
            pd.to_datetime("2025-01-01T01:00:00Z"),
            pd.to_datetime("2025-01-01T02:00:00Z"),
            pd.to_datetime("2025-01-01T03:00:00Z"),
        ],
        "solar_generation_kw": [5000.0, 6000.0, 2500.0, 3000.0],
        "region_id": [0, 0, 1, 1],
        "capacity_kw": [80_000_000, 80_000_000, 60_000_000, 60_000_000],
    },
    "capacity_updates": {
        "nl_national": 80_000_000_000,
        "nl_groningen": 60_000_000_000,
    },
    "id_column": "region_id",
}

BE_CONFIG = {
    "country": "be",
    "observer_name": "elia_be",
    "locations": [
        {
            "name": "be_belgium",
            "metadata_key": "region",
            "metadata_value": "belgium",
            "metadata_type": "string",
            "geometry": "POINT(4.35 50.85)",
            "capacity": 100_000_000,
        },
        {
            "name": "be_flanders",
            "metadata_key": "region",
            "metadata_value": "flanders",
            "metadata_type": "string",
            "geometry": "POINT(4.46 51.00)",
            "capacity": 100_000_000,
        },
        {
            "name": "be_wallonia",
            "metadata_key": "region",
            "metadata_value": "wallonia",
            "metadata_type": "string",
            "geometry": "POINT(4.70 50.50)",
            "capacity": 100_000_000,
        },
    ],
    "test_data": {
        "target_datetime_utc": [
            pd.to_datetime("2025-01-01T00:00:00Z"),
            pd.to_datetime("2025-01-01T01:00:00Z"),
            pd.to_datetime("2025-01-01T02:00:00Z"),
        ],
        "solar_generation_kw": [50000.0, 25000.0, 10000.0],
        "region": ["belgium", "flanders", "wallonia"],
        "forecast_type": ["generation", "generation", "generation"],
        "capacity_kw": [100.0 * 1000, 50.0 * 1000, 75.0 * 1000],
    },
    "capacity_updates": {},  # BE doesn't check capacity updates
    "id_column": "region",
}


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("config", [NL_CONFIG, BE_CONFIG], ids=["nl", "be"])
async def test_save_generation_to_data_platform(client, config):
    """
    Test saving generation data to the Data Platform.
    This test verifies that generation data is correctly stored for different countries.
    """
    country = config["country"]
    observer_name = config["observer_name"]
    
    # Create locations
    location_uuids = {}
    for loc_config in config["locations"]:
        metadata_fields = {
            "country": Value(string_value=country)
        }
        if loc_config["metadata_type"] == "number":
            metadata_fields[loc_config["metadata_key"]] = Value(number_value=loc_config["metadata_value"])
        else:
            metadata_fields[loc_config["metadata_key"]] = Value(string_value=loc_config["metadata_value"])

        metadata = Struct(fields=metadata_fields)
        
        location_type = dp.LocationType.NATION
        if loc_config.get("metadata_key") in ["region_id", "region"] and loc_config["name"] not in ["nl_national", "be_belgium"]:
            location_type = dp.LocationType.COUNTY

        create_location_request = dp.CreateLocationRequest(
            location_name=loc_config["name"],
            energy_source=dp.EnergySource.SOLAR,
            geometry_wkt=loc_config["geometry"],
            location_type=location_type,
            effective_capacity_watts=loc_config["capacity"],
            metadata=metadata,
            valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        )
        create_location_response = await client.create_location(create_location_request)
        location_uuids[loc_config["name"]] = create_location_response.location_uuid

    # Create observer
    create_observer_request = dp.CreateObserverRequest(name=observer_name)
    await client.create_observer(create_observer_request)

    # Create fake generation data
    fake_data = pd.DataFrame(config["test_data"])
    
    # Save the data to data platform
    await save_generation_to_data_platform(fake_data, client=client, country=country)

    # Verify observations were created for each location
    for location_name, location_uuid in location_uuids.items():
        get_observations_request = dp.GetObservationsAsTimeseriesRequest(
            location_uuid=location_uuid,
            observer_name=observer_name,
            energy_source=dp.EnergySource.SOLAR,
            time_window=dp.TimeWindow(
                start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
                end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
            ),
        )
        get_observations_response = await client.get_observations_as_timeseries(
            get_observations_request
        )
        
        # Check that observations exist
        assert len(get_observations_response.values) > 0, f"No observations found for {location_name}"

    # Verify location capacities were updated where expected
    for location_name, expected_capacity in config.get("capacity_updates", {}).items():
        location_uuid = location_uuids[location_name]
        # Use a pivot time after the update to ensure we see the new capacity
        pivot_time = datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc)
        get_location_request = dp.GetLocationRequest(
            location_uuid=location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            pivot_timestamp_utc=pivot_time
        )
        get_location_response = await client.get_location(get_location_request)    
        assert get_location_response.effective_capacity_watts == expected_capacity, \
            f"Capacity not updated correctly for {location_name}"


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("country,observer_name,id_column,test_value", [
    ("nl", "nednl", "region_id", 0),
    ("be", "elia_be", "region", "belgium"),
], ids=["nl", "be"])
async def test_save_generation_no_matching_locations(client, country, observer_name, id_column, test_value):
    """
    Test saving generation data when no matching locations exist.
    The function should create default locations and then save data.
    """
    # Create the required observer if it doesn't exist
    list_observer_request = dp.ListObserversRequest(observer_names_filter=[observer_name])
    list_observer_response = await client.list_observers(list_observer_request)
    if not any(obs.observer_name == observer_name for obs in list_observer_response.observers):
        create_observer_request = dp.CreateObserverRequest(name=observer_name)
        await client.create_observer(create_observer_request)

    # Create fake generation data
    if country == "nl":
        fake_data = pd.DataFrame({
            "target_datetime_utc": [pd.to_datetime("2025-01-01T00:00:00Z")],
            "solar_generation_kw": [5000.0],
            "region_id": [test_value],
            "capacity_kw": [80_000_000],
        })
        expected_location_name = "nl_national"
    else:  # be
        fake_data = pd.DataFrame({
            "target_datetime_utc": [pd.to_datetime("2025-01-01T00:00:00Z")],
            "solar_generation_kw": [50000.0],
            "region": [test_value],
            "forecast_type": ["generation"],
            "capacity_kw": [100.0 * 1000],
        })
        expected_location_name = "be_belgium"

    # Save the data - this should create default locations
    await save_generation_to_data_platform(fake_data, client=client, country=country)

    # Verify that default locations were created and data was saved
    list_locations_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.NATION,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    list_locations_response = await client.list_locations(list_locations_request)

    locations_data = list_locations_response.to_dict(
        casing=betterproto.Casing.SNAKE, include_default_values=True
    ).get("locations", [])

    # Verify locations exist (they may have been created earlier in test run)
    assert len(locations_data) > 0, "No locations found after save_generation_to_data_platform"

    # Check that expected location exists and has data
    target_location = None
    for loc in locations_data:
        if loc.get("location_name") == expected_location_name:
            target_location = loc
            break

    assert target_location is not None, f"{expected_location_name} location not found"
    
    # Verify country metadata is present
    metadata = target_location.get("metadata", {})
    country_meta = metadata.get("country", {}).get("string_value")
    assert country_meta == country, f"Country metadata missing or incorrect for {expected_location_name}"

    # Verify observations exist
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=target_location["location_uuid"],
        observer_name=observer_name,
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
        ),
    )

    get_observations_response = await client.get_observations_as_timeseries(
        get_observations_request
    )

    assert len(get_observations_response.values) >= 1, "No observations found for target location"


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("country", ["nl", "be"], ids=["nl", "be"])
async def test_save_generation_empty_dataframe(client, country):
    """
    Test saving empty generation data.
    Should handle gracefully without errors.
    """
    if country == "nl":
        empty_data = pd.DataFrame(
            columns=["target_datetime_utc", "solar_generation_kw", "region_id", "capacity_kw"]
        )
    else:  # be
        empty_data = pd.DataFrame(
            columns=["target_datetime_utc", "solar_generation_kw", "region", "forecast_type", "capacity_kw"]
        )

    # This should not raise an error
    await save_generation_to_data_platform(empty_data, client=client, country=country)


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("country,observer_name,id_column,metadata_key,metadata_value", [
    ("nl", "nednl", "region_id", "region_id", 99),
    ("be", "elia_be", "region", "region", "TestRegion"),
], ids=["nl", "be"])
async def test_save_generation_zero_capacity(client, country, observer_name, id_column, metadata_key, metadata_value):
    """
    Test saving generation data with zero capacity locations.
    Zero capacity locations should be filtered out.
    """
    # Create a location if it doesn't exist
    list_locations_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.NATION,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    list_locations_response = await client.list_locations(list_locations_request)
    locations_data = list_locations_response.to_dict(
        casing=betterproto.Casing.SNAKE, include_default_values=True
    ).get("locations", [])
    
    location_name = f"{country}_zero_capacity_test"
    location_exists = any(loc.get("location_name") == location_name for loc in locations_data)
    
    if not location_exists:
        if isinstance(metadata_value, int):
            metadata = Struct(fields={metadata_key: Value(number_value=metadata_value)})
        else:
            metadata = Struct(fields={metadata_key: Value(string_value=metadata_value)})
        
        create_location_request = dp.CreateLocationRequest(
            location_name=location_name,
            energy_source=dp.EnergySource.SOLAR,
            location_type=dp.LocationType.NATION,
            geometry_wkt="POINT(5.00 50.00)",
            effective_capacity_watts=100_000_000,
            metadata=metadata,
            valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        )
        create_location_response = await client.create_location(create_location_request)
        location_uuid = create_location_response.location_uuid
    else:
        target_location = next(loc for loc in locations_data if loc.get("location_name") == location_name)
        location_uuid = target_location['location_uuid']

    # Create observer if it doesn't exist
    list_observer_request = dp.ListObserversRequest(observer_names_filter=[observer_name])
    list_observer_response = await client.list_observers(list_observer_request)
    if not any(obs.observer_name == observer_name for obs in list_observer_response.observers):
        create_observer_request = dp.CreateObserverRequest(name=observer_name)
        await client.create_observer(create_observer_request)

    # Create data with zero capacity
    if country == "nl":
        fake_data = pd.DataFrame({
            "target_datetime_utc": [pd.to_datetime("2025-01-02T00:00:00Z")],
            "solar_generation_kw": [5000.0],
            "region_id": [metadata_value],
            "capacity_kw": [0.0],  # Zero capacity
        })
    else:  # be
        fake_data = pd.DataFrame({
            "target_datetime_utc": [pd.to_datetime("2025-01-02T00:00:00Z")],
            "solar_generation_kw": [50000.0],
            "region": [metadata_value],
            "forecast_type": ["generation"],
            "capacity_kw": [0.0],  # Zero capacity
        })

    # Save the data
    await save_generation_to_data_platform(fake_data, client=client, country=country)

    # Verify no observations were created (zero capacity filtered out)
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=location_uuid,
        observer_name=observer_name,
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
            end_timestamp_utc=datetime.datetime(2025, 1, 3, tzinfo=datetime.timezone.utc),
        ),
    )

    get_observations_response = await client.get_observations_as_timeseries(
        get_observations_request
    )

    assert len(get_observations_response.values) == 0, "Observations created for zero capacity location"


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("country,observer_name,id_column,id_value", [
    ("nl", "nednl", "region_id", 999),
    ("be", "elia_be", "region", "NonExistentRegion"),
], ids=["nl", "be"])
async def test_save_generation_missing_location_raises_error(client, country, observer_name, id_column, id_value):
    """
    Test that a ValueError is raised when trying to save data for a location that doesn't exist.
    This verifies the error handling when locations are unexpectedly missing.
    """
    # Create the required observer if it doesn't exist
    list_observer_request = dp.ListObserversRequest(observer_names_filter=[observer_name])
    list_observer_response = await client.list_observers(list_observer_request)
    if not any(obs.observer_name == observer_name for obs in list_observer_response.observers):
        create_observer_request = dp.CreateObserverRequest(name=observer_name)
        await client.create_observer(create_observer_request)

    # Create fake generation data for a non-existent location
    if country == "nl":
        fake_data = pd.DataFrame({
            "target_datetime_utc": [pd.to_datetime("2025-01-03T00:00:00Z")],
            "solar_generation_kw": [5000.0],
            "region_id": [id_value],
            "capacity_kw": [80_000_000],
        })
    else:  # be
        fake_data = pd.DataFrame({
            "target_datetime_utc": [pd.to_datetime("2025-01-03T00:00:00Z")],
            "solar_generation_kw": [50000.0],
            "region": [id_value],
            "forecast_type": ["generation"],
            "capacity_kw": [100.0 * 1000],
        })

    # Attempt to save data - should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        await save_generation_to_data_platform(fake_data, client=client, country=country)
    
    # Verify the error message contains expected information
    error_message = str(exc_info.value)
    assert f"No matching {country.upper()} locations found" in error_message
    assert id_column in error_message
    assert str(id_value) in error_message
    assert "unexpected" in error_message.lower()


import pandas as pd
import pytest
import pytest_asyncio
import time
import datetime
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from betterproto.lib.google.protobuf import Struct, Value
import betterproto
from importlib.metadata import version

from solar_consumer.save.save_data_platform import save_be_generation_to_data_platform

from dp_sdk.ocf import dp
from grpclib.client import Channel


@pytest_asyncio.fixture(scope="session")
async def client():
    """
    Fixture to spin up a PostgreSQL container for the entire test session.
    This fixture uses `testcontainers` to start a fresh PostgreSQL container and provides
    the connection URL dynamically for use in other fixtures.
    """

    # we use a specific postgres image with postgis and pgpartman installed
    # TODO make a release of this, not using logging tag.
    with PostgresContainer(
        "ghcr.io/openclimatefix/data-platform-pgdb:logging",
        username="postgres",
        password="postgres",
        dbname="postgres",
        env={"POSTGRES_HOST": "db"},
    ) as postgres:
        database_url = postgres.get_connection_url()
        # we need to get ride of psycopg2, so the go driver works
        database_url = database_url.replace("postgresql+psycopg2", "postgres")
        # we need to change to host.docker.internal so the data platform container can see it
        # https://stackoverflow.com/questions/46973456/docker-access-localhost-port-from-container
        database_url = database_url.replace("localhost", "host.docker.internal")

        with DockerContainer(
            image=f"ghcr.io/openclimatefix/data-platform:{version('dp_sdk')}",
            env={"DATABASE_URL": database_url},
            ports=[50051]
        ) as data_platform_server:
            time.sleep(1)  # Give some time for the server to start

            port = data_platform_server.get_exposed_port(50051)
            host = data_platform_server.get_container_host_ip()
            channel = Channel(host=host, port=port)
            client = dp.DataPlatformDataServiceStub(channel)
            yield client
            channel.close()


@pytest.mark.asyncio(loop_scope="session")
async def test_save_be_generation_to_data_platform(client):
    """
    Test saving Belgian generation data to the Data Platform.
    This test verifies that Belgian solar generation data is correctly stored
    using region-based location matching.
    """

    # Create Belgium locations with region metadata
    belgium_regions = [
        {"name": "Belgium", "region": "Belgium", "latitude": 50.85, "longitude": 4.35},
        {"name": "Flanders", "region": "Flanders", "latitude": 51.00, "longitude": 4.46},
        {"name": "Wallonia", "region": "Wallonia", "latitude": 50.50, "longitude": 4.70},
        {"name": "Brussels", "region": "Brussels", "latitude": 50.85, "longitude": 4.35},
    ]

    location_uuids = {}
    for region_data in belgium_regions:
        location_name = f"be_{region_data['region'].lower().replace(' ', '_')}"
        metadata = Struct(fields={"region": Value(string_value=region_data["region"])})

        create_location_request = dp.CreateLocationRequest(
            location_name=location_name,
            energy_source=dp.EnergySource.SOLAR,
            location_type=dp.LocationType.NATION,
            geometry_wkt=f"POINT({region_data['longitude']} {region_data['latitude']})",
            effective_capacity_watts=100_000_000,  # 100 MW
            metadata=metadata,
            valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        )
        create_location_response = await client.create_location(create_location_request)
        location_uuids[region_data["region"]] = create_location_response.location_uuid

    # Create the required observer
    create_observer_request = dp.CreateObserverRequest(name="elia_be")
    await client.create_observer(create_observer_request)

    # Create fake Belgian generation data matching the expected format
    fake_data = pd.DataFrame({
        "target_datetime_utc": [
            pd.to_datetime("2025-01-01T00:00:00Z"),
            pd.to_datetime("2025-01-01T01:00:00Z"),
            pd.to_datetime("2025-01-01T02:00:00Z"),
        ],
        "solar_generation_kw": [50000.0, 25000.0, 10000.0],  # 50 MW, 25 MW, 10 MW
        "region": ["Belgium", "Flanders", "Wallonia"],
        "forecast_type": ["generation", "generation", "generation"],
        "capacity_mwp": [100.0, 50.0, 75.0],  # Different capacities for testing
    })

    # Save the data to data platform
    await save_be_generation_to_data_platform(fake_data, client=client)

    # Verify observations were created for each region
    for region, location_uuid in location_uuids.items():
        region_data = fake_data[fake_data["region"] == region]
        if not region_data.empty:
            # Get observations for this location
            get_observations_request = dp.GetObservationsAsTimeseriesRequest(
                location_uuid=location_uuid,
                observer_name="elia_be",
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
            assert len(get_observations_response.values) > 0, f"No observations found for {region}"

            # Verify the values are correct (generation in watts)
            expected_value_watts = int(region_data.iloc[0]["solar_generation_kw"] * 1000)
            actual_value_watts = int(get_observations_response.values[0].value_fraction * get_observations_response.values[0].effective_capacity_watts)
            assert actual_value_watts == expected_value_watts, f"Value mismatch for {region}"

    # Verify location capacities were updated where necessary
    for region, location_uuid in location_uuids.items():
        region_data = fake_data[fake_data["region"] == region]
        if not region_data.empty:
            expected_capacity_watts = int(region_data.iloc[0]["capacity_mwp"] * 1_000_000)

            get_location_request = dp.GetLocationRequest(
                location_uuid=location_uuid, energy_source=dp.EnergySource.SOLAR
            )
            get_location_response = await client.get_location(get_location_request)

            # Check if capacity was updated (only if different from initial)
            if expected_capacity_watts != 100_000_000:  # Initial capacity
                assert get_location_response.effective_capacity_watts == expected_capacity_watts, \
                    f"Capacity not updated correctly for {region}"


@pytest.mark.asyncio(loop_scope="session")
async def test_save_be_generation_no_matching_locations(client):
    """
    Test saving Belgian generation data when no matching locations exist.
    The function should create default locations and then save data.
    """

    # Don't create any locations initially - let the function create defaults

    # Create the required observer if it doesn't exist
    list_observer_request = dp.ListObserversRequest(observer_names_filter=["elia_be"])
    list_observer_response = await client.list_observers(list_observer_request)
    if not any(obs.observer_name == "elia_be" for obs in list_observer_response.observers):
        create_observer_request = dp.CreateObserverRequest(name="elia_be")
        await client.create_observer(create_observer_request)

    # Create fake Belgian generation data
    fake_data = pd.DataFrame({
        "target_datetime_utc": [pd.to_datetime("2025-01-01T00:00:00Z")],
        "solar_generation_kw": [50000.0],
        "region": ["Belgium"],
        "forecast_type": ["generation"],
        "capacity_mwp": [100.0],
    })

    # Save the data - this should create default locations
    await save_be_generation_to_data_platform(fake_data, client=client)

    # Verify that default locations were created and data was saved
    list_locations_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.NATION,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    list_locations_response = await client.list_locations(list_locations_request)

    locations_data = list_locations_response.to_dict(
        casing=betterproto.Casing.SNAKE, include_default_values=True
    ).get("locations", [])

    # Should have created default Belgium locations
    assert len(locations_data) >= 4, "Default Belgium locations were not created"

    # Check that Belgium location exists and has data
    belgium_location = None
    for loc in locations_data:
        if loc.get("location_name") == "be_belgium":
            belgium_location = loc
            break

    assert belgium_location is not None, "Belgium location not found"

    # Verify observations exist for Belgium
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=belgium_location["location_uuid"],
        observer_name="elia_be",
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
        ),
    )

    get_observations_response = await client.get_observations_as_timeseries(
        get_observations_request
    )

    assert len(get_observations_response.values) == 1
    assert int(get_observations_response.values[0].value_fraction * get_observations_response.values[0].effective_capacity_watts) == 50_000_000  # 50 MW in watts


@pytest.mark.asyncio(loop_scope="session")
async def test_save_be_generation_empty_dataframe(client):
    """
    Test saving empty Belgian generation data.
    Should handle gracefully without errors.
    """

    # Create fake empty data
    empty_data = pd.DataFrame(
        columns=["target_datetime_utc", "solar_generation_kw", "region", "forecast_type", "capacity_mwp"]
    )

    # This should not raise an error
    await save_be_generation_to_data_platform(empty_data, client=client)


@pytest.mark.asyncio(loop_scope="session")
async def test_save_be_generation_zero_capacity(client):
    """
    Test saving Belgian generation data with zero capacity locations.
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
    
    location_exists = any(loc.get("location_name") == "be_belgium_zero_test" for loc in locations_data)
    
    if not location_exists:
        metadata = Struct(fields={"region": Value(string_value="Belgium")})
        create_location_request = dp.CreateLocationRequest(
            location_name="be_belgium_zero_test",
            energy_source=dp.EnergySource.SOLAR,
            location_type=dp.LocationType.NATION,
            geometry_wkt="POINT(4.35 50.85)",
            effective_capacity_watts=100_000_000,
            metadata=metadata,
            valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        )
        create_location_response = await client.create_location(create_location_request)
    else:
        # Find existing location
        belgium_location = next(loc for loc in locations_data if loc.get("location_name") == "be_belgium_zero_test")
        create_location_response = type('MockResponse', (), {'location_uuid': belgium_location['location_uuid']})()

    # Create observer if it doesn't exist
    list_observer_request = dp.ListObserversRequest(observer_names_filter=["elia_be"])
    list_observer_response = await client.list_observers(list_observer_request)
    if not any(obs.observer_name == "elia_be" for obs in list_observer_response.observers):
        create_observer_request = dp.CreateObserverRequest(name="elia_be")
        await client.create_observer(create_observer_request)

    # Create data with zero capacity
    fake_data = pd.DataFrame({
        "target_datetime_utc": [pd.to_datetime("2025-01-02T00:00:00Z")],
        "solar_generation_kw": [50000.0],
        "region": ["Belgium"],
        "forecast_type": ["generation"],
        "capacity_mwp": [0.0],  # Zero capacity
    })

    # Save the data
    await save_be_generation_to_data_platform(fake_data, client=client)

    # Verify no observations were created (zero capacity filtered out)
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=create_location_response.location_uuid,
        observer_name="elia_be",
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
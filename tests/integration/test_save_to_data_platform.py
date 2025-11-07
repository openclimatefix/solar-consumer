import pandas as pd
import numpy as np
import pytest
import pytest_asyncio
import time
import datetime
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from betterproto.lib.google.protobuf import Struct, Value

from solar_consumer.save.save_data_platform import save_generation_to_data_platform

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
            image="ghcr.io/openclimatefix/data-platform:0.10.0", env={"DATABASE_URL": database_url}, ports=[50051]
        ) as data_platform_server:
            time.sleep(1)  # Give some time for the server to start

            port = data_platform_server.get_exposed_port(50051)
            host = data_platform_server.get_container_host_ip()
            channel = Channel(host=host, port=port)
            client = dp.DataPlatformDataServiceStub(channel)
            yield client
            channel.close()




@pytest.mark.asyncio(loop_scope="session")
async def test_save_to_data_platform(client):
    """
    Test saving data to the Data Platform.
    This test uses the `data_platform` fixture to ensure that the Data Platform service
    is running and can accept data.
    """

    # add location
    metadata = Struct(fields={"gsp_id": Value(number_value=1)})
    create_location_request = dp.CreateLocationRequest(
        location_name="gsp1",
        energy_source=dp.EnergySource.SOLAR,
        geometry_wkt="POINT(0 0)",
        location_type=dp.LocationType.GSP,
        effective_capacity_watts=1_000_000,
        metadata=metadata,
        valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
    )
    create_location_response = await client.create_location(create_location_request)
    location_uuid = create_location_response.location_uuid

    # make fake data
    fake_data = pd.DataFrame(
        {"target_datetime_utc": ["2025-01-01T00:00:00Z"], "solar_generation_kw": [100]}
    )
    fake_data["gsp_id"] = 1
    fake_data["regime"] = "in-day"
    fake_data["capacity_mwp"] = 2
    fake_data["target_datetime_utc"] = pd.to_datetime(fake_data["target_datetime_utc"])

    _ = await save_generation_to_data_platform(fake_data, client=client)

    # read from the data platform to check it was saved
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=location_uuid,
        observer_name="pvlive_consumer_in_day",
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
    # check fraction value is 100 kw / 2 mwp = 0.05
    assert (
        np.abs(get_observations_response.values[0].value_fraction - 0.05) < 1e-6
    )

    # check location capacity has been updated
    get_location_request = dp.GetLocationRequest(
        location_uuid=location_uuid, energy_source=dp.EnergySource.SOLAR
    )
    get_location_response = await client.get_location(get_location_request)
    assert get_location_response.effective_capacity_watts == 2_000_000

import asyncio
import datetime
from importlib.metadata import version
from pathlib import Path

import betterproto
import pandas as pd
import pytest_asyncio
from betterproto.lib.google.protobuf import Struct, Value
from grpclib.client import Channel
from ocf import dp
from testcontainers.core.container import DockerContainer
from testcontainers.postgres import PostgresContainer

LOCATIONS_CSV_PATH = (
    Path(__file__).parent.parent.parent / "solar_consumer" / "data" / "locations.csv"
)


@pytest_asyncio.fixture(scope="module")
async def client():
    """
    Fixture to spin up a PostgreSQL container and Data Platform container for each test module.
    This fixture uses `testcontainers` to start fresh containers and provides
    the data platform client dynamically for use in integration tests.
    """

    # we use a specific postgres image with postgis and pgpartman installed
    # TODO make a release of this, not using logging tag.
    with PostgresContainer(
        f"ghcr.io/openclimatefix/data-platform-pgdb:{version('dp_sdk')}",
        username="postgres",
        password="postgres",
        dbname="postgres",
        env={"POSTGRES_HOST": "db"},
    ) as postgres:
        database_url = postgres.get_connection_url()
        # we need to get rid of psycopg2, so the go driver works
        database_url = database_url.replace("postgresql+psycopg2", "postgres")
        # we need to change to host.docker.internal so the data platform container can see it
        # https://stackoverflow.com/questions/46973456/docker-access-localhost-port-from-container
        database_url = database_url.replace("localhost", "host.docker.internal")

        with DockerContainer(
            image=f"ghcr.io/openclimatefix/data-platform:{version('dp_sdk')}",
            env={"DATABASE_URL": database_url},
            ports=[50051],
        ) as data_platform_server:
            await asyncio.sleep(1)  # Give some time for the server to start

            port = data_platform_server.get_exposed_port(50051)
            host = data_platform_server.get_container_host_ip()
            channel = Channel(host=host, port=port)
            client = dp.DataPlatformDataServiceStub(channel)
            yield client
            channel.close()


async def _create_locations_from_csv(
    client: dp.DataPlatformDataServiceStub,
    country: str,
    capacity_watts_by_id: dict | None = None,
) -> dict[str, str]:
    """Create locations from CSV file for integration tests."""
    capacity_watts_by_id = capacity_watts_by_id or {}

    locations_df_csv = pd.read_csv(LOCATIONS_CSV_PATH)
    locations_df_csv = locations_df_csv[locations_df_csv["country_code"] == country]
    locations = locations_df_csv.to_dict(orient="records")

    # Fetch existing locations to avoid duplicate creation when tests share container
    existing_locations_data = []
    for loc_type in (dp.LocationType.NATION, dp.LocationType.STATE):
        for es in (dp.EnergySource.SOLAR, dp.EnergySource.WIND):
            resp = await client.list_locations(
                dp.ListLocationsRequest(
                    location_type_filter=loc_type,
                    energy_source_filter=es,
                )
            )
            existing_locations_data.extend(
                resp.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True).get(
                    "locations", []
                )
            )

    existing_by_name: dict[str, str] = {
        loc["location_name"]: loc["location_uuid"]
        for loc in existing_locations_data
        if "location_name" in loc and "location_uuid" in loc
    }
    existing_sources: set[tuple[str, str]] = {
        (loc["location_name"], loc.get("energy_source", "").upper())
        for loc in existing_locations_data
        if "location_name" in loc
    }

    created_uuids: dict[str, str] = {}
    for location in locations:
        location_name = location["name"]
        location_type_str = location.get("location_type", "NATION")
        if location_type_str == "STATE":
            location_type = dp.LocationType.STATE
        else:
            location_type = dp.LocationType.NATION

        metadata_fields = {
            "country": Value(string_value=country),
        }
        if country in ["nl", "nl_no_curtailment"]:
            id_key = "region_id"
            id_value = int(location[id_key])
            metadata_fields[id_key] = Value(number_value=id_value)
        elif country in ["be", "de"]:
            id_key = "region"
            id_value = str(location[id_key])
            metadata_fields[id_key] = Value(string_value=id_value)
        elif country == "ind_rajasthan":
            id_key = "name"
            id_value = str(location[id_key])
            metadata_fields[id_key] = Value(string_value=id_value)
        else:
            id_key = "name"
            id_value = str(location[id_key])
            metadata_fields[id_key] = Value(string_value=id_value)

        effective_capacity_watts = int(capacity_watts_by_id.get(id_value, 100_000_000))
        metadata = Struct(fields=metadata_fields)

        energy_source_str = str(location.get("energy_source", "solar")).lower()
        if energy_source_str == "wind":
            energy_source = dp.EnergySource.WIND
        else:
            energy_source = dp.EnergySource.SOLAR

        if (location_name, energy_source.name) in existing_sources:
            created_uuids[location_name] = existing_by_name[location_name]
            continue

        if location_name in created_uuids or location_name in existing_by_name:
            loc_uuid = created_uuids.get(location_name) or existing_by_name[location_name]
            created_uuids[location_name] = loc_uuid
            await client.create_location_energy_source(
                dp.CreateLocationEnergySourceRequest(
                    location_uuid=loc_uuid,
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
        existing_by_name[location_name] = create_location_response.location_uuid
        existing_sources.add((location_name, energy_source.name))

    return created_uuids


@pytest_asyncio.fixture(scope="module")
async def setup_locations_from_csv(client):
    """Fixture providing a helper to create locations from the locations CSV."""
    async def _setup(country: str, capacity_watts_by_id: dict | None = None) -> dict[str, str]:
        return await _create_locations_from_csv(client, country, capacity_watts_by_id)

    return _setup

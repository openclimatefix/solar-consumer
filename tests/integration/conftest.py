import pytest_asyncio
import time
from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from importlib.metadata import version

from dp_sdk.ocf import dp
from grpclib.client import Channel


@pytest_asyncio.fixture(scope="session")
async def client():
    """
    Fixture to spin up a PostgreSQL container and Data Platform container for the entire test session.
    This fixture uses `testcontainers` to start fresh containers and provides
    the data platform client dynamically for use in integration tests.
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
            time.sleep(1)  # Give some time for the server to start

            port = data_platform_server.get_exposed_port(50051)
            host = data_platform_server.get_container_host_ip()
            channel = Channel(host=host, port=port)
            client = dp.DataPlatformDataServiceStub(channel)
            yield client
            channel.close()

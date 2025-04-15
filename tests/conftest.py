import pytest
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from nowcasting_datamodel.models.base import Base_Forecast
from nowcasting_datamodel.models import MLModelSQL
from testcontainers.postgres import PostgresContainer

# Shared Test Configuration Constants
RESOURCE_ID = "db6c038f-98af-4570-ab60-24d71ebd0ae5"
LIMIT = 5
MODEL_NAME = "real_data_model"
MODEL_VERSION = "1.0"


@pytest.fixture(scope="session")
def postgres_container():
    """
    Fixture to spin up a PostgreSQL container for the entire test session.
    This fixture uses `testcontainers` to start a fresh PostgreSQL container and provides
    the connection URL dynamically for use in other fixtures.
    """
    with PostgresContainer("postgres:15.5") as postgres:
        postgres.start()
        yield postgres.get_connection_url()


@pytest.fixture(scope="function")
def db_session(postgres_container) -> Generator:
    """
    Fixture to set up and tear down a PostgreSQL database session for testing.
    This fixture:
    - Connects to the PostgreSQL container provided by `postgres_container`.
    - Creates a fresh database schema before each test.
    - Adds a dummy ML model for test purposes.
    - Tears down the database session and cleans up resources after each test.

    Args:
        postgres_container (str): The dynamic connection URL provided by PostgresContainer.

    Returns:
        Generator: A SQLAlchemy session object.
    """
    # Use the dynamic connection URL
    engine = create_engine(postgres_container)
    Base_Forecast.metadata.drop_all(engine)  # Drop all tables to ensure a clean slate
    Base_Forecast.metadata.create_all(engine)  # Recreate the tables

    # Establish session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Insert a dummy model for testing
    session.query(MLModelSQL).delete()  # Clean up any pre-existing data
    model = MLModelSQL(name=MODEL_NAME, version=MODEL_VERSION)
    session.add(model)
    session.commit()

    yield session  # Provide the session to the test

    # Cleanup: close session and dispose of engine
    session.close()
    engine.dispose()


@pytest.fixture(scope="session")
def test_config():
    """
    Fixture to provide shared test configuration constants.

    Returns:
        dict: A dictionary of test configuration values.
    """
    return {
        "resource_id": RESOURCE_ID,
        "limit": LIMIT,
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "csv_dir": "test_csv",  # Directory for CSV saving
    }

# Mocking the NL response data
@pytest.fixture(scope="function")
def nl_mock_data():
    return {
    "hydra:member": [
        {
            "id": 1,
            "point": 0,
            "type": 2,
            "granularity": 4,
            "activity": 1,
            "classification": 2,
            "capacity": 1000,
            "volume": 500,
            "percentage": 50,
            "validfrom": "2025-04-01T00:00:00+00:00",
            "validto": "2025-04-01T01:00:00+00:00",
            "lastupdate": "2025-04-01T01:30:00+00:00"
        }
    ]
}
from datetime import UTC, datetime, timedelta

import pytest

# Shared Test Configuration Constants
RESOURCE_ID = "db6c038f-98af-4570-ab60-24d71ebd0ae5"
LIMIT = 5
MODEL_NAME = "real_data_model"
MODEL_VERSION = "1.0"


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
    now = datetime.now(UTC)
    return {
    "hydra:member": [
        {
            "id": 1,
            "point": "v1/points/0",
            "type": 2,
            "granularity": 4,
            "activity": 1,
            "classification": 2,
            "capacity": 1000,
            "volume": 500,
            "percentage": 50,
            "validfrom": (now - timedelta(hours=1)).isoformat(),
            "validto": (now - timedelta(minutes=45)).isoformat(),
            "lastupdate": (now.isoformat())
        }
    ]
}

# Mocking the NL response data
@pytest.fixture(scope="function")
def nl_mock_data_small_percentage():
    now = datetime.now(UTC)
    return {
    "hydra:member": [
        {
            "id": 1,
            "point": "v1/points/0",
            "type": 2,
            "granularity": 4,
            "activity": 1,
            "classification": 2,
            "capacity": 1000,
            "volume": 500,
            "percentage": 0.001,
            "validfrom": (now - timedelta(hours=1)).isoformat(),
            "validto": (now - timedelta(minutes=45)).isoformat(),
            "lastupdate": (now.isoformat())
        }
    ]
}
import pandas as pd
import pytest
from unittest.mock import AsyncMock, MagicMock

from solar_consumer.save.save_data_platform import save_be_generation_to_data_platform


@pytest.mark.asyncio
async def test_save_be_generation_empty_data():
    """
    Test saving empty DataFrame - should not raise error and not create observations.
    """

    mock_client = AsyncMock()

    # Mock the required methods to avoid errors
    mock_client.list_observers.return_value = MagicMock(observers=[])
    mock_client.create_observer.return_value = None
    mock_client.list_locations.return_value = MagicMock(locations=[])
    mock_client.create_location.return_value = MagicMock(location_uuid="test-uuid")

    # Empty test data
    test_data = pd.DataFrame(
        columns=["target_datetime_utc", "solar_generation_kw", "region", "forecast_type", "capacity_mwp"]
    )

    # Should not raise error
    await save_be_generation_to_data_platform(test_data, mock_client)

    # Verify no observations were created
    mock_client.create_observations.assert_not_called()


@pytest.mark.asyncio
async def test_save_be_generation_zero_capacity_filtered():
    """
    Test that locations with zero capacity are filtered out.
    """

    mock_client = AsyncMock()

    # Mock observer exists
    mock_observer_response = MagicMock()
    mock_observer = MagicMock()
    mock_observer.observer_name = "elia_be"
    mock_observer_response.observers = [mock_observer]
    mock_client.list_observers.return_value = mock_observer_response

    # Mock existing locations
    mock_location_response = MagicMock()
    mock_location_response.to_dict.return_value = {
        "locations": [{
            "location_uuid": "existing-uuid",
            "location_name": "be_belgium", 
            "metadata": {"region": {"string_value": "Belgium"}},
            "effective_capacity_watts": 100_000_000
        }]
    }
    mock_client.list_locations.return_value = mock_location_response

    # Test data with zero capacity
    test_data = pd.DataFrame({
        "target_datetime_utc": [pd.to_datetime("2025-01-01T00:00:00Z")],
        "solar_generation_kw": [50000.0],
        "region": ["Belgium"],
        "forecast_type": ["generation"],
        "capacity_mwp": [0.0],  # Zero capacity
    })

    # Call the function
    await save_be_generation_to_data_platform(test_data, mock_client)

    # Verify no observations were created (filtered out)
    mock_client.create_observations.assert_not_called()
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from solar_consumer.save.save_data_platform import save_generation_to_data_platform

# the capacity already stored in the data platform, 58212 MW
STORED_CAPACITY_WATTS = 58_212_000_000
# the installed capacity ENTSO-E reports, which is far too high, so we ignore it
ENTSOE_CAPACITY_KW = 103_260_000

TIMESTAMPS = [pd.to_datetime("2025-01-01T10:00:00Z"), pd.to_datetime("2025-01-01T11:00:00Z")]


def make_mock_client() -> AsyncMock:
    """A data platform client with one German location at STORED_CAPACITY_WATTS."""
    mock_client = AsyncMock()

    mock_observer = MagicMock()
    mock_observer.observer_name = "entsoe_de"
    mock_client.list_observers.return_value = MagicMock(observers=[mock_observer])

    mock_location_response = MagicMock()
    mock_location_response.to_dict.return_value = {
        "locations": [
            {
                "location_uuid": "de-uuid",
                "location_name": "de_germany",
                "energy_source": "SOLAR",
                "metadata": {
                    "region": {"string_value": "de"},
                    "country": {"string_value": "de"},
                },
                "effective_capacity_watts": STORED_CAPACITY_WATTS,
            }
        ]
    }
    mock_client.list_locations.return_value = mock_location_response

    return mock_client


def make_de_data(max_generation_kw: float) -> pd.DataFrame:
    """German generation as ENTSO-E gives it to us, peaking at max_generation_kw first."""
    return pd.DataFrame(
        {
            "target_datetime_utc": TIMESTAMPS,
            "solar_generation_kw": [max_generation_kw, max_generation_kw / 2],
            "region": ["de", "de"],
            "capacity_kw": [float(ENTSOE_CAPACITY_KW)] * 2,
        }
    )


@pytest.mark.asyncio
async def test_save_de_generation_capacity_not_lowered():
    """Generation below the stored capacity leaves it alone, and the ENTSO-E capacity is ignored."""
    mock_client = make_mock_client()

    await save_generation_to_data_platform(
        make_de_data(max_generation_kw=40_000_000), mock_client, config_name="de"
    )

    mock_client.update_location.assert_not_called()


@pytest.mark.asyncio
async def test_save_de_generation_capacity_raised_to_max_generation():
    """Generation above the stored capacity rolls the capacity up to that new maximum."""
    mock_client = make_mock_client()

    await save_generation_to_data_platform(
        make_de_data(max_generation_kw=60_000_000), mock_client, config_name="de"
    )

    mock_client.update_location.assert_called_once()
    request = mock_client.update_location.call_args.args[0]
    assert request.new_effective_capacity_watts == 60_000_000_000
    # dated from the peak, not the end of the batch, so the peak fits under the new capacity
    assert request.valid_from_utc == TIMESTAMPS[0]

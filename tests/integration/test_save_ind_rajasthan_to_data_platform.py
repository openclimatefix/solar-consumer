import datetime

import betterproto
import pandas as pd
import pytest

from dp_sdk.ocf import dp
from solar_consumer.save.save_data_platform import save_generation_to_data_platform


COUNTRY = "ind_rajasthan"
OBSERVER_NAME = "ruvnl"


@pytest.mark.asyncio(loop_scope="module")
async def test_save_ind_rajasthan_generation_to_data_platform(client):
    """
    Test saving RUVNL (Rajasthan, India) solar and wind generation data to the
    Data Platform.

    No locations are pre-created: the save function should create both
    ``ruvnl_solar_site`` and ``ruvnl_wind_site`` from the locations CSV, then
    store one observation per energy_type against the matching location.
    """
    # Fake generation data containing one solar and one wind row, matching the
    # shape produced by ``fetch_ind_rajasthan_data``.
    fake_data = pd.DataFrame(
        {
            "energy_type": ["solar", "wind"],
            "target_datetime_utc": [
                pd.to_datetime("2025-01-01T00:00:00Z"),
                pd.to_datetime("2025-01-01T00:00:00Z"),
            ],
            "solar_generation_kw": [1500.0, 800.0],
            "capacity_kw": [5000.0, 3000.0],
        }
    )

    # Save the data - should create locations from CSV then write observations.
    await save_generation_to_data_platform(fake_data, client=client, country=COUNTRY)

    # Verify both sites were created from the CSV.
    list_locations_response = await client.list_locations(
        dp.ListLocationsRequest(
            location_type_filter=dp.LocationType.STATE,
            energy_source_filter=dp.EnergySource.SOLAR,
        )
    )
    locations_data = list_locations_response.to_dict(
        casing=betterproto.Casing.SNAKE, include_default_values=True
    ).get("locations", [])

    sites_by_name = {
        loc["location_name"]: loc
        for loc in locations_data
        if loc.get("location_name") in {"ruvnl_solar_site", "ruvnl_wind_site"}
        and loc.get("metadata", {}).get("country", {}).get("string_value") == COUNTRY
    }

    assert "ruvnl_solar_site" in sites_by_name, "ruvnl_solar_site was not created"
    assert "ruvnl_wind_site" in sites_by_name, "ruvnl_wind_site was not created"

    # Verify observations exist for each site under the ruvnl observer.
    time_window = dp.TimeWindow(
        start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
        end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
    )

    expected_watts = {
        "ruvnl_solar_site": 1_500_000,  # 1500 kW -> W
        "ruvnl_wind_site": 800_000,     # 800 kW -> W
    }

    for name, expected in expected_watts.items():
        location_uuid = sites_by_name[name]["location_uuid"]
        observations_response = await client.get_observations_as_timeseries(
            dp.GetObservationsAsTimeseriesRequest(
                location_uuid=location_uuid,
                observer_name=OBSERVER_NAME,
                energy_source=dp.EnergySource.SOLAR,
                time_window=time_window,
            )
        )
        assert len(observations_response.values) >= 1, f"No observations found for {name}"
        values_watts = [
            int(round(v.value_fraction * v.effective_capacity_watts))
            for v in observations_response.values
        ]
        assert expected in values_watts, (
            f"Expected observation {expected} W for {name}, got {values_watts}"
        )


@pytest.mark.asyncio(loop_scope="module")
async def test_save_ind_rajasthan_empty_dataframe(client):
    """Empty input should be handled gracefully without raising."""
    empty_data = pd.DataFrame(
        columns=["energy_type", "target_datetime_utc", "solar_generation_kw", "capacity_kw"]
    )
    await save_generation_to_data_platform(empty_data, client=client, country=COUNTRY)

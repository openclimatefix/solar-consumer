import datetime

import betterproto
import pandas as pd
import pytest
from ocf import dp

from solar_consumer.save.save_data_platform import save_generation_to_data_platform

COUNTRY = "ind_rajasthan"
OBSERVER_NAME = "ruvnl"
LOCATION_NAME = "ruvnl"


@pytest.mark.asyncio(loop_scope="module")
async def test_save_ind_rajasthan_generation_to_data_platform(client):
    """
    Test saving RUVNL (Rajasthan, India) solar and wind generation data to the
    Data Platform.

    No locations are pre-created: the save function should create the ``ruvnl``
    location and both its energy sources from the locations CSV, then store one
    observation per energy_type against the matching source.
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
    await save_generation_to_data_platform(fake_data, client=client, config_name=COUNTRY)

    # Verify both locations were created from the CSV.
    # Query SOLAR and WIND separately since UNSPECIFIED doesn't match either.
    locations_data = []
    for es in (dp.EnergySource.SOLAR, dp.EnergySource.WIND):
        resp = await client.list_locations(
            dp.ListLocationsRequest(
                location_type_filter=dp.LocationType.STATE,
                energy_source_filter=es,
            )
        )
        locations_data.extend(
            resp.to_dict(
                casing=betterproto.Casing.SNAKE, include_default_values=True
            ).get("locations", [])
        )

    ruvnl_locations = [
        loc
        for loc in locations_data
        if loc.get("location_name") == LOCATION_NAME
        and loc.get("metadata", {}).get("country", {}).get("string_value") == COUNTRY
    ]

    location_uuids = {loc["location_uuid"] for loc in ruvnl_locations}
    assert location_uuids, f"{LOCATION_NAME} was not created"
    assert len(location_uuids) == 1, "solar and wind  should share one location"
    assert {loc["energy_source"] for loc in ruvnl_locations} == {"SOLAR", "WIND"}
    location_uuid = next(iter(location_uuids))

    # Each source carries its own capacity. Asserting this catches both a capacity taken
    # across all sources at once, and a concurrent update to the shared location getting lost.
    expected_capacity_watts = {"SOLAR": 5_000_000, "WIND": 3_000_000}
    for loc in ruvnl_locations:
        energy_source = loc["energy_source"]
        assert int(float(loc["effective_capacity_watts"])) == expected_capacity_watts[energy_source]

    # Verify observations exist for each energy source under the ruvnl observer.
    time_window = dp.TimeWindow(
        start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
        end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.UTC),
    )

    expected_watts = {
        dp.EnergySource.SOLAR: 1_500_000,   # 1500 kW -> W
        dp.EnergySource.WIND: 800_000,       # 800 kW -> W
    }

    for energy_source, expected in expected_watts.items():
        observations_response = await client.get_observations_as_timeseries(
            dp.GetObservationsAsTimeseriesRequest(
                location_uuid=location_uuid,
                observer_name=OBSERVER_NAME,
                energy_source=energy_source,
                time_window=time_window,
            )
        )
        assert len(observations_response.values) >= 1, f"No observations found for {energy_source.name}"
        values_watts = [
            round(v.value_fraction * v.effective_capacity_watts)
            for v in observations_response.values
        ]
        assert expected in values_watts, (
            f"Expected observation {expected} W for {energy_source.name}, got {values_watts}"
        )


@pytest.mark.asyncio(loop_scope="module")
async def test_save_ind_rajasthan_empty_dataframe(client):
    """Empty input should be handled gracefully without raising."""
    empty_data = pd.DataFrame(
        columns=["energy_type", "target_datetime_utc", "solar_generation_kw", "capacity_kw"]
    )
    await save_generation_to_data_platform(empty_data, client=client, config_name=COUNTRY)

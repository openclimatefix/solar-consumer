import datetime

import betterproto
import pandas as pd
import pytest

from ocf import dp
from solar_consumer.save.save_data_platform import save_generation_to_data_platform


COUNTRY = "de"
OBSERVER_NAME = "entsoe_de"

# region -> (capacity_kw, [generation_kw at t0, t1, t2])
DE_FAKE = {
    "de": (103_260_000, [30_000_000, 50_000_000, 40_000_000]),
    "50hertz": (25_235_000, [7_000_000, 12_000_000, 9_000_000]),
    "amprion": (24_487_000, [6_000_000, 10_000_000, 8_000_000]),
    "tennet": (40_140_000, [10_000_000, 18_000_000, 14_000_000]),
    "transnetbw": (13_398_000, [3_000_000, 6_000_000, 4_000_000]),
}
TIMESTAMPS = [
    pd.to_datetime("2025-01-01T10:00:00Z"),
    pd.to_datetime("2025-01-01T11:00:00Z"),
    pd.to_datetime("2025-01-01T12:00:00Z"),
]


@pytest.mark.asyncio(loop_scope="module")
async def test_save_de_generation_to_data_platform(client):
    """
    Test saving German (per-TSO + national) solar generation to the Data Platform.

    No locations are pre-created: the save function should create the German locations from the
    locations CSV (seeded with real installed capacity), then store one observation per region
    and timestamp against the matching location.
    """
    rows = []
    for region, (capacity_kw, gens) in DE_FAKE.items():
        for ts, gen in zip(TIMESTAMPS, gens):
            rows.append(
                {
                    "region": region,
                    "tso_zone": None,
                    "target_datetime_utc": ts,
                    "solar_generation_kw": float(gen),
                    "capacity_kw": float(capacity_kw),
                }
            )
    fake_data = pd.DataFrame(rows)

    await save_generation_to_data_platform(fake_data, client=client, config_name=COUNTRY)

    # Fetch the German locations that were created.
    locations_data = []
    for loc_type in (dp.LocationType.NATION, dp.LocationType.STATE):
        resp = await client.list_locations(
            dp.ListLocationsRequest(
                location_type_filter=loc_type,
                energy_source_filter=dp.EnergySource.SOLAR,
            )
        )
        locations_data.extend(
            resp.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True).get(
                "locations", []
            )
        )

    loc_by_region = {
        loc.get("metadata", {}).get("region", {}).get("string_value"): loc
        for loc in locations_data
        if loc.get("metadata", {}).get("country", {}).get("string_value") == COUNTRY
    }

    time_window = dp.TimeWindow(
        start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
        end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
    )

    for region, (_, gens) in DE_FAKE.items():
        assert region in loc_by_region, f"{region} location was not created"
        location_uuid = loc_by_region[region]["location_uuid"]
        observations_response = await client.get_observations_as_timeseries(
            dp.GetObservationsAsTimeseriesRequest(
                location_uuid=location_uuid,
                observer_name=OBSERVER_NAME,
                energy_source=dp.EnergySource.SOLAR,
                time_window=time_window,
            )
        )
        # all three timestamps stored
        assert len(observations_response.values) >= len(TIMESTAMPS), (
            f"Expected >= {len(TIMESTAMPS)} observations for {region}, "
            f"got {len(observations_response.values)}"
        )
        values_watts = [
            v.value_fraction * v.effective_capacity_watts for v in observations_response.values
        ]
        # Values are stored as a fraction of capacity, so allow a small rounding tolerance.
        for gen in gens:
            expected = gen * 1000
            assert any(abs(v - expected) <= max(1000, expected * 1e-3) for v in values_watts), (
                f"Expected observation ~{expected} W for {region}, got {values_watts}"
            )


@pytest.mark.asyncio(loop_scope="module")
async def test_save_de_empty_dataframe(client):
    """Empty input should be handled gracefully without raising."""
    empty_data = pd.DataFrame(
        columns=["region", "tso_zone", "target_datetime_utc", "solar_generation_kw", "capacity_kw"]
    )
    await save_generation_to_data_platform(empty_data, client=client, config_name=COUNTRY)

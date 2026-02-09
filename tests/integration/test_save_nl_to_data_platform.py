import pandas as pd
import pytest
import datetime
from betterproto.lib.google.protobuf import Struct, Value

from solar_consumer.save.save_data_platform import save_generation_to_data_platform

from dp_sdk.ocf import dp


@pytest.mark.asyncio(loop_scope="session")
async def test_save_nl_to_data_platform(client):
    """
    Test saving NL data to the Data Platform.
    This test verifies that the NL branch of save_generation_to_data_platform works correctly.
    """

    # add NL location with region_id 0 (national)
    metadata = Struct(fields={"region_id": Value(number_value=0)})
    create_location_request = dp.CreateLocationRequest(
        location_name="nl_national",
        energy_source=dp.EnergySource.SOLAR,
        geometry_wkt="POINT(5.29 52.13)",
        location_type=dp.LocationType.NATION,
        effective_capacity_watts=100_000_000_000,
        metadata=metadata,
        valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
    )
    create_location_response = await client.create_location(create_location_request)
    location_uuid_national = create_location_response.location_uuid

    # add NL location with region_id 1 (groningen)
    metadata = Struct(fields={"region_id": Value(number_value=1)})
    create_location_request = dp.CreateLocationRequest(
        location_name="nl_groningen",
        energy_source=dp.EnergySource.SOLAR,
        geometry_wkt="POINT(6.74 53.22)",
        location_type=dp.LocationType.NATION,
        effective_capacity_watts=50_000_000_000,
        metadata=metadata,
        valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
    )
    create_location_response = await client.create_location(create_location_request)
    location_uuid_groningen = create_location_response.location_uuid

    # add observer for NL
    create_observer_request = dp.CreateObserverRequest(
        name="nednl",
    )
    _ = await client.create_observer(create_observer_request)

    # make fake NL data
    fake_data = pd.DataFrame(
        {
            "target_datetime_utc": [
                pd.to_datetime("2025-01-01T00:00:00Z"),
                pd.to_datetime("2025-01-01T01:00:00Z"),
                pd.to_datetime("2025-01-01T00:00:00Z"),
                pd.to_datetime("2025-01-01T01:00:00Z"),
            ],
            "solar_generation_kw": [5000.0, 6000.0, 2500.0, 3000.0],
            "region_id": [0, 0, 1, 1],
            "capacity_kw": [80_000_000, 80_000_000, 60_000_000, 60_000_000],
        }
    )
    _ = await save_generation_to_data_platform(fake_data, client=client, country="nld")

    # read from the data platform to check national data was saved
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=location_uuid_national,
        observer_name="nednl",
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
        ),
    )
    get_observations_response = await client.get_observations_as_timeseries(
        get_observations_request
    )
    assert len(get_observations_response.values) == 2

    # read from the data platform to check groningen data was saved
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=location_uuid_groningen,
        observer_name="nednl",
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
        ),
    )
    get_observations_response = await client.get_observations_as_timeseries(
        get_observations_request
    )
    assert len(get_observations_response.values) == 2

    # check national location capacity has been updated (from 100B to 80B * 1000 = 80B)
    get_location_request = dp.GetLocationRequest(
        location_uuid=location_uuid_national, energy_source=dp.EnergySource.SOLAR
    )
    get_location_response = await client.get_location(get_location_request)
    assert get_location_response.effective_capacity_watts == 80_000_000_000

    # check groningen location capacity has been updated (from 50B to 60B * 1000 = 60B)
    get_location_request = dp.GetLocationRequest(
        location_uuid=location_uuid_groningen, energy_source=dp.EnergySource.SOLAR
    )
    get_location_response = await client.get_location(get_location_request)
    assert get_location_response.effective_capacity_watts == 60_000_000_000

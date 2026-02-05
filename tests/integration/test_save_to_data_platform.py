import pandas as pd
import numpy as np
import pytest
import datetime
from betterproto.lib.google.protobuf import Struct, Value

from solar_consumer.save.save_data_platform import save_generation_to_data_platform

from dp_sdk.ocf import dp


@pytest.mark.asyncio(loop_scope="module")
async def test_save_to_data_platform(client):
    """
    Test saving data to the Data Platform.
    This test uses the `data_platform` fixture to ensure that the Data Platform service
    is running and can accept data.
    """

    # add location gsp 0
    metadata = Struct(fields={"gsp_id": Value(number_value=0)})
    create_location_request = dp.CreateLocationRequest(
        location_name="uk",
        energy_source=dp.EnergySource.SOLAR,
        geometry_wkt="POINT(0 0)",
        location_type=dp.LocationType.NATION,
        effective_capacity_watts=10_000_000,
        metadata=metadata,
        valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
    )
    create_location_response = await client.create_location(create_location_request)

    # add location gsp 1
    metadata = Struct(fields={"gsp_id": Value(number_value=1)})
    create_location_request = dp.CreateLocationRequest(
        location_name="gsp1",
        energy_source=dp.EnergySource.SOLAR,
        geometry_wkt="POINT(0 0)",
        location_type=dp.LocationType.GSP,
        effective_capacity_watts=1_000_000,
        metadata=metadata,
        valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
    )
    create_location_response = await client.create_location(create_location_request)
    location_uuid = create_location_response.location_uuid

    # add observer
    create_observer_request = dp.CreateObserverRequest(
        name="pvlive_in_day",
    )
    _ = await client.create_observer(create_observer_request)

    # make fake data
    fake_data = pd.DataFrame(
        {
            "target_datetime_utc": [pd.to_datetime("2025-01-01T00:00:00Z")],
            "solar_generation_kw": [100.0],
            "gsp_id": [1],
            "regime": ["in-day"],
            "capacity_mwp": [2],
        }
    )
    _ = await save_generation_to_data_platform(fake_data, client=client)

    # read from the data platform to check it was saved
    get_observations_request = dp.GetObservationsAsTimeseriesRequest(
        location_uuid=location_uuid,
        observer_name="pvlive_in_day",
        energy_source=dp.EnergySource.SOLAR,
        time_window=dp.TimeWindow(
            start_timestamp_utc=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            end_timestamp_utc=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
        ),
    )

    get_observations_response = await client.get_observations_as_timeseries(
        get_observations_request
    )
    assert len(get_observations_response.values) == 1
    # check fraction value is 100 kw / 2 mwp = 0.05
    assert np.abs(get_observations_response.values[0].value_fraction - 0.05) < 1e-6

    # check location capacity has been updated
    get_location_request = dp.GetLocationRequest(
        location_uuid=location_uuid, energy_source=dp.EnergySource.SOLAR
    )
    get_location_response = await client.get_location(get_location_request)
    assert get_location_response.effective_capacity_watts == 2_000_000

import pandas as pd
import numpy as np
import pytest
import datetime
from betterproto.lib.google.protobuf import Struct, Value

from solar_consumer.save.save_data_platform import (
    get_update_capacity_df,
    save_generation_to_data_platform,
    save_forecasts_to_data_platform,
)

from dp_sdk.ocf import dp


@pytest.mark.asyncio(loop_scope="module")
async def test_save_to_data_platform(client):
    """
    Test saving data to the Data Platform.
    This test uses the `data_platform` fixture to ensure that the Data Platform service
    is running and can accept data.
    """

    # add location gsp 1
    metadata = Struct(fields={"gsp_id": Value(number_value=1), 
                              "full_name": Value(string_value="test_1")})
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
    # note that the second date point is above 110% of capacity, so wont be added 
    fake_data = pd.DataFrame(
        {
            "target_datetime_utc": [pd.to_datetime("2025-01-01T00:00:00Z"), pd.to_datetime("2025-01-01T00:00:00Z")],
            "solar_generation_kw": [100.0, 3000.0],
            "gsp_id": [1, 1],
            "regime": ["in-day", "in-day"],
            "capacity_kw": [2000, 2000],
            "capacity_no_degradation_kw": [2200, 2200]
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
    metadata_dict = get_location_response.metadata.to_dict()
    assert metadata_dict["capacity_no_degradation_kw"]['numberValue'] == 2_200
    print(metadata_dict["full_name"])
    assert metadata_dict["full_name"]['stringValue'] == "test_1"


@pytest.mark.asyncio(loop_scope="module")
async def test_save_forecasts_to_data_platform(client):
    """
    Test saving forecast data to the Data Platform.
    """
    # 1. Create a national location for GB
    metadata = Struct(
        fields={"gsp_id": Value(number_value=0), "full_name": Value(string_value="National")}
    )
    effective_capacity_watts = 1_000_000
    create_location_request = dp.CreateLocationRequest(
        location_name="uk",
        energy_source=dp.EnergySource.SOLAR,
        location_type=dp.LocationType.NATION,
        effective_capacity_watts=effective_capacity_watts,
        geometry_wkt="POINT(0 0)",
        metadata=metadata,
        valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
    )
    create_location_response = await client.create_location(create_location_request)
    location_uuid = create_location_response.location_uuid

    # 2. Prepare test data
    model_tag = "test_model"
    model_version = "1.0.0"

    start_time = pd.to_datetime("2026-03-26T12:00:00Z")
    data_df = pd.DataFrame(
        {
            "target_datetime_utc": [
                start_time + datetime.timedelta(minutes=x) for x in range(0, 60, 30)
            ],
            "solar_generation_kw": [100.0, 500.0],
        }
    )

    # 3. Call the function
    await save_forecasts_to_data_platform(
        data_df=data_df,
        client=client,
        model_tag=model_tag,
        model_version=model_version,
        init_time_utc=start_time,
        country="gb",
    )

    # 4. Verify the forecast
    # Get the forecaster
    list_forecasters_response = await client.list_forecasters(
        dp.ListForecastersRequest(forecaster_names_filter=[model_tag.replace("-", "_")])
    )

    assert len(list_forecasters_response.forecasters) == 1
    forecaster = list_forecasters_response.forecasters[0]
    assert forecaster.forecaster_version == model_version

    # Get the forecast back from the data platform
    get_latest_forecasts_request = dp.GetLatestForecastsRequest(
        energy_source=dp.EnergySource.SOLAR,
        pivot_timestamp_utc=start_time + datetime.timedelta(days=1),
        location_uuid=location_uuid,
    )
    get_latest_forecasts_response = await client.get_latest_forecasts(
        get_latest_forecasts_request,
    )
    assert len(get_latest_forecasts_response.forecasts) == 1
    forecast = get_latest_forecasts_response.forecasts[0]

    stream_forecast_data_request = dp.StreamForecastDataRequest(
        energy_source=dp.EnergySource.SOLAR,
        location_uuid=location_uuid,
        forecasters=forecast.forecaster,
        time_window=dp.TimeWindow(
            start_timestamp_utc=start_time,
            end_timestamp_utc=start_time + datetime.timedelta(hours=2),
        ),
    )
    stream_forecast_data_response = client.stream_forecast_data(
        stream_forecast_data_request,
    )

    values = []
    async for d in stream_forecast_data_response:
        values.append(d)

    assert len(values) == 2

    # Check values. a p50_fraction = solar_generation_kw * 1000 / effective_capacity_watts
    # value 1: 100 kw * 1000 W/kw / 1_000_000 W = 0.1
    # value 2: 500 kw * 1000 W/kw / 1_000_000 W = 0.5
    assert np.isclose(values[0].p50_fraction, 0.1)
    assert np.isclose(values[1].p50_fraction, 0.5)

    forecasts = await client.get_forecast_as_timeseries(
        dp.GetForecastAsTimeseriesRequest(
            location_uuid=location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            forecaster=forecast.forecaster,
            time_window=dp.TimeWindow(
                start_timestamp_utc=start_time,
                end_timestamp_utc=start_time + datetime.timedelta(hours=2),
            ),
        )
    )
    assert len(forecasts.values) == 2

def test_get_update_capacity_df():

    # test_cases
    # 1 is the same
    test_1 = {'effective_capacity_watts': 1, 
                    'new_effective_capacity_watts': 1, 
                    'target_datetime_utc': pd.to_datetime("2026-03-26T12:00:00Z")}
    # 2 is over 0.1% but under 1 MW
    test_2 = {'effective_capacity_watts': 2, 
                    'new_effective_capacity_watts': 4, 
                    'target_datetime_utc': pd.to_datetime("2026-03-26T12:30:00Z")}
    # 3 is under 0.1% but over 1 MW
    test_3 = {'effective_capacity_watts': 1e12, 
                    'new_effective_capacity_watts': 1.0001e12, 
                    'target_datetime_utc': pd.to_datetime("2026-03-26T13:00:00Z")}
    # 4 is under 0.1%, and under 1 MW
    test_4 = {'effective_capacity_watts': 10_000, 
                    'new_effective_capacity_watts': 10_001, 
                    'target_datetime_utc': pd.to_datetime("2026-03-26T13:30:00Z")}
    
    df = pd.DataFrame([test_1, test_2, test_3, test_4])
    updates_df = get_update_capacity_df(df)
    print(updates_df)
    assert not updates_df.empty
    assert updates_df.index.tolist() == [1, 2]



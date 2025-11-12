from solar_consumer.save.save_site_database import save_generation_to_site_db, save_forecasts_to_site_db
from solar_consumer.save.save_data_platform import save_generation_to_data_platform
from pvsite_datamodel.sqlmodels import GenerationSQL, ForecastSQL, ForecastValueSQL, LocationSQL
import pandas as pd

import unittest
from unittest.mock import AsyncMock, patch
import dataclasses
import uuid
from dp_sdk.ocf import dp
from betterproto.lib.google.protobuf import Struct, Value
import numpy as np


def test_save_generation_to_site_db(db_site_session):

    """
    Test the save_generation_to_site_db function.
    """
    # Prepare mock data
    generation_data = {
        "target_datetime_utc": ["2023-10-01 00:00:00", "2023-10-01 01:00:00"],
        "solar_generation_kw": [100, 150],
        "capacity_kw": [20_000_000, 20_000_002],
        "region_id": [0,0]
    }

    # Convert to DataFrame
    generation_df = pd.DataFrame(generation_data)

    # Call the function
    save_generation_to_site_db(
        generation_data=generation_df,
        session=db_site_session,
    )

    # Check if data is saved correctly in the database
    saved_data = db_site_session.query(GenerationSQL).all()

    assert len(saved_data) == len(generation_df)

    sites = db_site_session.query(LocationSQL).all()
    assert len(sites) == 1
    assert sites[0].capacity_kw == 20_000_002
    assert sites[0].client_location_name == "nl_national"


def test_save_forecasts_to_site_db(db_site_session):

    """
    Test the save_generation_to_site_db function.
    """
    # Prepare mock data
    forecast_data = {
        "target_datetime_utc": ["2023-10-01 00:00:00+00:00", "2023-10-01 01:00:00+00:00"],
        "solar_generation_kw": [100, 150],
        "capacity_kw": [200, 201],
    }

    # Convert to DataFrame
    forecast_df = pd.DataFrame(forecast_data)
    forecast_df["target_datetime_utc"] = pd.to_datetime(forecast_df["target_datetime_utc"])

    # Call the function
    save_forecasts_to_site_db(
        forecast_data=forecast_df,
        session=db_site_session,
        model_tag="test-model",
        model_version="1.0",
    )

    # Check if data is saved correctly in the database
    assert len(db_site_session.query(ForecastSQL).all()) == 1
    saved_data = db_site_session.query(ForecastValueSQL).all()

    assert len(saved_data) == len(forecast_df)


class TestSaveGenerationToDataPlatform(unittest.IsolatedAsyncioTestCase):

    @patch("dp_sdk.ocf.dp.DataPlatformDataServiceStub")
    async def test_save_generation_to_data_platform(self, client_mock):
        # Mock the list_locations call to return one national and three GSP locations
        # * The GSPs all have 1MW capacity and the nation has 100MW
        def mock_list_locations(req: dp.ListLocationsRequest):
            if req.location_type_filter == dp.LocationType.GSP:
                return dp.ListLocationsResponse(
                    locations=[
                        dp.ListLocationsResponseLocationSummary(
                            location_name=f"mock_gsp_{i}",
                            location_uuid=str(uuid.uuid4()),
                            energy_source=dp.EnergySource.SOLAR,
                            effective_capacity_watts=1e6,
                            location_type=dp.LocationType.GSP,
                            latlng=dp.LatLng(51.5, -0.1),
                            metadata=Struct(fields={"gsp_id": Value(number_value=i)}),)
                        for i in range(1, 4)
                    ]
                )
            elif req.location_type_filter == dp.LocationType.NATION:
                return dp.ListLocationsResponse(
                    locations=[
                        dp.ListLocationsResponseLocationSummary(
                            location_name="mock_uk",
                            location_uuid=str(uuid.uuid4()),
                            energy_source=dp.EnergySource.SOLAR,
                            effective_capacity_watts=100e6,
                            location_type=dp.LocationType.NATION,
                            latlng=dp.LatLng(52.5, -1.5),
                            metadata=Struct(fields={"gsp_id": Value(number_value=0)}),
                        )
                    ]
                )
            else:   
                return dp.ListLocationsResponse(locations=[])


        @dataclasses.dataclass
        class TestCase:
            name: str
            input_df: pd.DataFrame
            expected_update_capacities: list[float]
            expected_create_call_observation_counts: list[int]
            should_error: bool

        testcases: list[TestCase] = [
            TestCase(
                name="Missing GSP ID in input, no capacity updates",
                input_df=pd.DataFrame({
                    "gsp_id": [0, 0, 2, 2, 3, 3],
                    "regime": ["test"] * 6,
                    "capacity_mwp": [100, 100, 1, 1, 1, 1],
                    "solar_generation_kw": [5e3, 5e3, 25, 25, 0, 0],
                    "target_datetime_utc": [
                        np.datetime64('2023-01-01T00:00:00'),
                        np.datetime64('2023-01-01T01:00:00'),
                        np.datetime64('2023-01-01T00:00:00'),
                        np.datetime64('2023-01-01T01:00:00'),
                        np.datetime64('2023-01-01T00:00:00'),
                        np.datetime64('2023-01-01T01:00:00'),
                    ],
                }),
                expected_update_capacities=[],
                expected_create_call_observation_counts=[2, 2, 2],
                should_error=False,
            ),
            TestCase(
                name="One GSP capacity update, one national",
                input_df=pd.DataFrame({
                    "gsp_id": [0, 1],
                    "regime": ["test"] * 2,
                    "capacity_mwp": [100, 5],
                    "solar_generation_kw": [5e3, 50],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=2, freq="h"),
                }),
                expected_update_capacities=[5e6],
                expected_create_call_observation_counts=[1, 1],
                should_error=False,
            ),
            TestCase(
                name="Zero capacity GSPs are skipped",
                input_df=pd.DataFrame({
                    "gsp_id": [1, 1, 2, 2],
                    "regime": ["test"] * 4,
                    "capacity_mwp": [10, 10, 0, 0],
                    "solar_generation_kw": [50, 50, 10, 10],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=4, freq="h"),
                }),
                expected_update_capacities=[10e6],
                expected_create_call_observation_counts=[2],
                should_error=False,
            ),
            TestCase(
                name="GSP ID not in dataplatform is ignored",
                input_df=pd.DataFrame({
                    "gsp_id": [1, 99],
                    "regime": ["test"] * 2,
                    "capacity_mwp": [1, 10],
                    "solar_generation_kw": [50, 100],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=2, freq="h"),
                }),
                expected_update_capacities=[],
                expected_create_call_observation_counts=[1],
                should_error=False,
            ),
            TestCase(
                name="Only latest capacity is used for update call",
                input_df=pd.DataFrame({
                    "gsp_id": [1, 1, 1],
                    "regime": ["test"] * 3,
                    "capacity_mwp": [5, 10, 2],
                    "solar_generation_kw": [50, 100, 20],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=3, freq="h"),
                }),
                expected_update_capacities=[2e6],
                expected_create_call_observation_counts=[3],
                should_error=False,
            ),
        ]


        for case in testcases:
            client_mock.list_locations = AsyncMock(side_effect=mock_list_locations)
            client_mock.update_location_capacity = AsyncMock()
            client_mock.create_observations = AsyncMock()

            with self.subTest(case.name):
                if not case.should_error:
                    await save_generation_to_data_platform(case.input_df, client_mock)
                    self.assertEqual(
                        client_mock.update_location_capacity.call_count,
                        len(case.expected_update_capacities),
                    )
                    self.assertEqual(
                        client_mock.create_observations.call_count,
                        len(case.expected_create_call_observation_counts),
                    )
                else:
                    with self.assertRaises(Exception):
                        await save_generation_to_data_platform(case.input_df, client_mock)


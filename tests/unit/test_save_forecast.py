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


def test_save_generation_to_site_db_none_capacity(db_site_session):
    """
    Test the save_generation_to_site_db function.

    Add None in capacity_kw column
    """
    # Prepare mock data
    generation_data = {
        "target_datetime_utc": ["2023-10-01 00:00:00", "2023-10-01 01:00:00"],
        "solar_generation_kw": [100, 150],
        "capacity_kw": [None, None],
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
    # default capacity used
    assert sites[0].capacity_kw == 20_000_000
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
        def mock_list_locations(req: dp.ListLocationsRequest) -> dp.ListLocationsResponse:
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

        def mock_list_observers(req: dp.ListObserversRequest) -> dp.ListObserversResponse:
            return dp.ListObserversResponse(
                observers=[
                    dp.ListObserversResponseObserverSummary(
                        observer_uuid=str(uuid.uuid4()),
                        observer_name=name,
                    )
                    for name in ["pvlive_in_day", "pvlive_day_ahead"]
                ]
            )

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
                    "capacity_kw": [100e3, 100e3, 1e3, 1e3, 1e3, 1e3],
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
                    "capacity_kw": [100e3, 0.2e3],
                    "solar_generation_kw": [5e3, 50],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=2, freq="h"),
                }),
                expected_update_capacities=[2e5],
                expected_create_call_observation_counts=[1, 1],
                should_error=False,
            ),
            TestCase(
                name="Zero capacity GSPs are skipped",
                input_df=pd.DataFrame({
                    "gsp_id": [1, 1, 2, 2],
                    "regime": ["test"] * 4,
                    "capacity_kw": [10e3, 10e3, 0, 0],
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
                    "capacity_kw": [1e3, 10e3],
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
                    "capacity_kw": [5e3, 10e3, 2e3],
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
            client_mock.update_location = AsyncMock()
            client_mock.create_observations = AsyncMock()
            client_mock.list_observers = AsyncMock(side_effect=mock_list_observers)
            client_mock.create_observer = AsyncMock()

            with self.subTest(case.name):
                if not case.should_error:
                    await save_generation_to_data_platform(case.input_df, client_mock)
                    # Assert the data platform functioms were called the expected number of times
                    self.assertEqual(
                        client_mock.update_location.call_count,
                        len(case.expected_update_capacities),
                    )
                    self.assertEqual(
                        client_mock.create_observations.call_count,
                        len(case.expected_create_call_observation_counts),
                    )

                    # Assert the expected arguments were passed to the data platform functions
                    for call, expected_capacity in zip(
                        client_mock.update_location.call_args_list,
                        case.expected_update_capacities,
                    ):
                        actual_capacity = call.args[0].new_effective_capacity_watts
                        self.assertEqual(actual_capacity, expected_capacity)

                    for call, expected_count in zip(
                        client_mock.create_observations.call_args_list,
                        case.expected_create_call_observation_counts,
                    ):
                        actual_count = len(call.args[0].values)
                        self.assertEqual(actual_count, expected_count)
                else:
                    with self.assertRaises(Exception):
                        await save_generation_to_data_platform(case.input_df, client_mock)

    @patch("dp_sdk.ocf.dp.DataPlatformDataServiceStub")
    async def test_save_nl_generation_to_data_platform(self, client_mock):
        """Test the NL branch of save_generation_to_data_platform."""

        # Mock the list_locations call to return NL locations with region_id metadata
        def mock_list_locations(req: dp.ListLocationsRequest) -> dp.ListLocationsResponse:
            if req.location_type_filter == dp.LocationType.NATION:
                return dp.ListLocationsResponse(
                    locations=[
                        dp.ListLocationsResponseLocationSummary(
                            location_name="nl_national",
                            location_uuid=str(uuid.uuid4()),
                            energy_source=dp.EnergySource.SOLAR,
                            effective_capacity_watts=100_000_000_000,
                            location_type=dp.LocationType.NATION,
                            latlng=dp.LatLng(52.13, 5.29),
                            metadata=Struct(fields={"region_id": Value(number_value=0), "country": Value(string_value="nl")}),
                        ),
                        dp.ListLocationsResponseLocationSummary(
                            location_name="nl_groningen",
                            location_uuid=str(uuid.uuid4()),
                            energy_source=dp.EnergySource.SOLAR,
                            effective_capacity_watts=50_000_000_000,
                            location_type=dp.LocationType.NATION,
                            latlng=dp.LatLng(53.22, 6.74),
                            metadata=Struct(fields={"region_id": Value(number_value=1), "country": Value(string_value="nl")}),
                        ),
                        dp.ListLocationsResponseLocationSummary(
                            location_name="nl_friesland",
                            location_uuid=str(uuid.uuid4()),
                            energy_source=dp.EnergySource.SOLAR,
                            effective_capacity_watts=25_000_000_000,
                            location_type=dp.LocationType.NATION,
                            latlng=dp.LatLng(53.11, 5.85),
                            metadata=Struct(fields={"region_id": Value(number_value=2), "country": Value(string_value="nl")}),
                        ),
                    ]
                )
            else:
                return dp.ListLocationsResponse(locations=[])

        def mock_list_observers(req: dp.ListObserversRequest) -> dp.ListObserversResponse:
            return dp.ListObserversResponse(
                observers=[
                    dp.ListObserversResponseObserverSummary(
                        observer_uuid=str(uuid.uuid4()),
                        observer_name="nednl",
                    )
                ]
            )

        @dataclasses.dataclass
        class TestCase:
            name: str
            input_df: pd.DataFrame
            expected_update_capacities: list[float]
            expected_create_call_observation_counts: list[int]
            should_error: bool

        testcases: list[TestCase] = [
            TestCase(
                name="NL: Basic test with matching region_ids",
                input_df=pd.DataFrame({
                    "region_id": [0, 0, 1, 1],
                    "capacity_kw": [100_000_000, 100_000_000, 50_000_000, 50_000_000],
                    "solar_generation_kw": [5000, 6000, 2500, 3000],
                    "target_datetime_utc": [
                        np.datetime64('2023-01-01T00:00:00'),
                        np.datetime64('2023-01-01T01:00:00'),
                        np.datetime64('2023-01-01T00:00:00'),
                        np.datetime64('2023-01-01T01:00:00'),
                    ],
                }),
                expected_update_capacities=[],
                expected_create_call_observation_counts=[2, 2],
                should_error=False,
            ),
            TestCase(
                name="NL: Capacity update when different from data platform",
                input_df=pd.DataFrame({
                    "region_id": [0, 1],
                    "capacity_kw": [80_000_000, 60_000_000],  # Different from mock (100B and 50B)
                    "solar_generation_kw": [5000, 2500],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=2, freq="h"),
                }),
                expected_update_capacities=[80_000_000_000, 60_000_000_000],
                expected_create_call_observation_counts=[1, 1],
                should_error=False,
            ),
            TestCase(
                name="NL: Zero capacity regions are skipped",
                input_df=pd.DataFrame({
                    "region_id": [0, 0, 1, 1],
                    "capacity_kw": [100_000_000, 100_000_000, 0, 0],
                    "solar_generation_kw": [5000, 6000, 2500, 3000],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=4, freq="h"),
                }),
                expected_update_capacities=[],
                expected_create_call_observation_counts=[2],
                should_error=False,
            ),
            TestCase(
                name="NL: Region ID not in data platform is ignored",
                input_df=pd.DataFrame({
                    "region_id": [0, 99],
                    "capacity_kw": [100_000_000, 100_000_000],
                    "solar_generation_kw": [5000, 6000],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=2, freq="h"),
                }),
                expected_update_capacities=[],
                expected_create_call_observation_counts=[1],
                should_error=False,
            ),
            TestCase(
                name="NL: Only latest capacity is used for update call",
                input_df=pd.DataFrame({
                    "region_id": [0, 0, 0],
                    "capacity_kw": [50_000_000, 80_000_000, 60_000_000],
                    "solar_generation_kw": [5000, 6000, 7000],
                    "target_datetime_utc": pd.date_range(start="2023-01-01", periods=3, freq="h"),
                }),
                expected_update_capacities=[60_000_000_000],
                expected_create_call_observation_counts=[3],
                should_error=False,
            ),
        ]

        for case in testcases:
            client_mock.list_locations = AsyncMock(side_effect=mock_list_locations)
            client_mock.update_location = AsyncMock()
            client_mock.create_observations = AsyncMock()
            client_mock.list_observers = AsyncMock(side_effect=mock_list_observers)
            client_mock.create_observer = AsyncMock()
            client_mock.create_location = AsyncMock()

            with self.subTest(case.name):
                if not case.should_error:
                    await save_generation_to_data_platform(case.input_df, client_mock, country="nl")
                    # Assert the data platform functions were called the expected number of times
                    self.assertEqual(
                        client_mock.update_location.call_count,
                        len(case.expected_update_capacities),
                    )
                    self.assertEqual(
                        client_mock.create_observations.call_count,
                        len(case.expected_create_call_observation_counts),
                    )

                    # Assert the expected arguments were passed to the data platform functions
                    for call, expected_capacity in zip(
                        client_mock.update_location.call_args_list,
                        case.expected_update_capacities,
                    ):
                        actual_capacity = call.args[0].new_effective_capacity_watts
                        self.assertEqual(actual_capacity, expected_capacity)

                    for call, expected_count in zip(
                        client_mock.create_observations.call_args_list,
                        case.expected_create_call_observation_counts,
                    ):
                        actual_count = len(call.args[0].values)
                        self.assertEqual(actual_count, expected_count)

                    # Verify observer name is "nednl" for NL
                    for call in client_mock.create_observations.call_args_list:
                        self.assertEqual(call.args[0].observer_name, "nednl")
                else:
                    with self.assertRaises(Exception):
                        await save_generation_to_data_platform(case.input_df, client_mock, country="nl")

    @patch("dp_sdk.ocf.dp.DataPlatformDataServiceStub")
    async def test_save_nl_generation_creates_locations_when_none_exist(self, client_mock):
        """Test that NL locations are created from CSV when none exist in data platform."""

        call_count = 0

        def mock_list_locations(req: dp.ListLocationsRequest) -> dp.ListLocationsResponse:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call returns empty (no locations exist)
                return dp.ListLocationsResponse(locations=[])
            else:
                # Second call (after creation) returns the created locations
                all_locations = [
                        dp.ListLocationsResponseLocationSummary(
                            location_name="nl_national",
                            location_uuid=str(uuid.uuid4()),
                            energy_source=dp.EnergySource.SOLAR,
                            effective_capacity_watts=100_000_000_000,
                            location_type=dp.LocationType.NATION,
                            latlng=dp.LatLng(52.13, 5.29),
                            metadata=Struct(fields={"region_id": Value(number_value=0), "country": Value(string_value="nl")}),
                        ),
                    ]
                
                filtered = [loc for loc in all_locations if loc.location_type == req.location_type_filter]
                return dp.ListLocationsResponse(locations=filtered)

        def mock_list_observers(req: dp.ListObserversRequest) -> dp.ListObserversResponse:
            return dp.ListObserversResponse(
                observers=[
                    dp.ListObserversResponseObserverSummary(
                        observer_uuid=str(uuid.uuid4()),
                        observer_name="nednl",
                    )
                ]
            )

        client_mock.list_locations = AsyncMock(side_effect=mock_list_locations)
        client_mock.create_location = AsyncMock()
        client_mock.update_location = AsyncMock()
        client_mock.create_observations = AsyncMock()
        client_mock.list_observers = AsyncMock(side_effect=mock_list_observers)
        client_mock.create_observer = AsyncMock()

        input_df = pd.DataFrame({
            "region_id": [0],
            "capacity_kw": [100_000_000],
            "solar_generation_kw": [5000],
            "target_datetime_utc": [np.datetime64('2023-01-01T00:00:00')],
        })

        await save_generation_to_data_platform(input_df, client_mock, country="nl")

        # Verify create_location was called for each location in the CSV (13 locations)
        self.assertEqual(client_mock.create_location.call_count, 13)

        # Verify list_locations was called twice (once before, once after creation)
        self.assertEqual(client_mock.list_locations.call_count, 4)

def test_save_generation_to_site_db_ind_rajasthan(db_site_session):
    generation_data = {
        "target_datetime_utc": ["2023-10-01 00:00:00", "2023-10-01 01:00:00"],
        "solar_generation_kw": [100, 200],
        "capacity_kw": [500, 600],
        "energy_type": ["solar", "wind"],
    }

    generation_df = pd.DataFrame(generation_data)

    save_generation_to_site_db(
        generation_data=generation_df,
        session=db_site_session,
        country="ind_rajasthan",
    )

    saved_data = db_site_session.query(GenerationSQL).all()
    assert len(saved_data) == len(generation_df)

    sites = db_site_session.query(LocationSQL).all()
    site_names = sorted([site.client_location_name for site in sites])

    assert site_names == ["runvl_solar_site", "runvl_wind_site"]

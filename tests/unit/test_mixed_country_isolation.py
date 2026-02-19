import unittest
from unittest.mock import AsyncMock, patch
import pandas as pd
from dp_sdk.ocf import dp
from betterproto.lib.google.protobuf import Struct, Value
import uuid

# Import the function to test
from solar_consumer.save.save_data_platform import save_generation_to_data_platform

class TestMixedCountryHandling(unittest.IsolatedAsyncioTestCase):
    @patch("dp_sdk.ocf.dp.DataPlatformDataServiceStub")
    async def test_mixed_country_locations_isolation(self, client_mock):
        """
        Verify that operations for one country (NL) do not interact with locations 
        from another country (GB) even if they exist in the database.
        """
        
        # Setup: One GB location and one NL location in the "database"
        gb_uuid = str(uuid.uuid4())
        nl_uuid = str(uuid.uuid4())
        
        all_locations_db = [
            # GB Location
            dp.ListLocationsResponseLocationSummary(
                location_name="gb_gsp_0",
                location_uuid=gb_uuid,
                energy_source=dp.EnergySource.SOLAR,
                effective_capacity_watts=100_000_000,
                location_type=dp.LocationType.GSP,
                metadata=Struct(fields={"gsp_id": Value(number_value=0)}), # No country metadata = GB
            ),
            # NL Location
            dp.ListLocationsResponseLocationSummary(
                location_name="nl_national",
                location_uuid=nl_uuid,
                energy_source=dp.EnergySource.SOLAR,
                effective_capacity_watts=50_000_000,
                location_type=dp.LocationType.NATION,
                metadata=Struct(fields={
                    "region_id": Value(number_value=0),
                    "country": Value(string_value="nl")
                }),
            )
        ]
        
        # Mocking list_locations to simulate the data platform returning ALL locations correctly
        # The internal logic of save_generation_to_data_platform calls _list_locations which calls client.list_locations
        # We need to mock client.list_locations to return everything, and rely on the function under test to filter.
        
        def mock_list_locations_side_effect(req: dp.ListLocationsRequest) -> dp.ListLocationsResponse:
            # In a real scenario, the API might filter by type.
            # Here we return everything that matches query type to ensure our code filters by COUNTRY.
            
            filtered = []
            for loc in all_locations_db:
                # Basic type filtering simulation
                if req.location_type_filter == loc.location_type:
                    filtered.append(loc)
            
            return dp.ListLocationsResponse(locations=filtered)

        client_mock.list_locations = AsyncMock(side_effect=mock_list_locations_side_effect)
        client_mock.update_location = AsyncMock()
        client_mock.create_observations = AsyncMock()
        client_mock.list_observers = AsyncMock(return_value=dp.ListObserversResponse(observers=[]))
        client_mock.create_observer = AsyncMock()
        client_mock.create_location = AsyncMock()

        # Execute: Try to save data for NL
        nl_input_df = pd.DataFrame({
            "region_id": [0],
            "capacity_kw": [50_000], 
            "solar_generation_kw": [50],
            "target_datetime_utc": [pd.Timestamp("2023-01-01 12:00:00")],
        })
        
        await save_generation_to_data_platform(nl_input_df, client_mock, country="nl")
        
        # Assert: 
        # 1. Update/Create Obs called ONLY for NL UUID
        # We expect 0 updates (capacities match) and 1 observation creation
        
        self.assertEqual(client_mock.create_observations.call_count, 1)
        call_args = client_mock.create_observations.call_args[0][0]
        self.assertEqual(call_args.location_uuid, nl_uuid, "Should strictly interact with NL location")
        self.assertNotEqual(call_args.location_uuid, gb_uuid, "Should NOT touch GB location")

        # Execute: Try to save data for GB
        gb_input_df = pd.DataFrame({
            "gsp_id": [0],
            "regime": ["in-day"],
            "capacity_kw": [100_000],
            "solar_generation_kw": [80],
            "target_datetime_utc": [pd.Timestamp("2023-01-01 12:00:00")],
        })

        client_mock.create_observations.reset_mock()
        await save_generation_to_data_platform(gb_input_df, client_mock, country="gb")

        # Assert:
        # 1. Interact ONLY with GB UUID
        self.assertEqual(client_mock.create_observations.call_count, 1)
        call_args = client_mock.create_observations.call_args[0][0]
        self.assertEqual(call_args.location_uuid, gb_uuid, "Should strictly interact with GB location")
        self.assertNotEqual(call_args.location_uuid, nl_uuid, "Should NOT touch NL location")

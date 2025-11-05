"""Functions to save to the Data-platform

https://github.com/openclimatefix/data-platform

"""

import os
from loguru import logger

from dp_sdk.ocf import dp
from grpclib.client import Channel
import pandas as pd


data_platform_host = os.getenv("DATA_PLATFORM_HOST", "localhost")
data_platform_port = int(os.getenv("DATA_PLATFORM_PORT", "50051"))


async def save_to_generation_to_data_platform(data_df: pd.DataFrame):
    """
    Save Generation data to the Data-platform.

    0. First we get all the locations

    Here's how we do it for each gsp
    1. Get only the data for that gsp
    2. Get the start and end timestamps from that data
    3. Get the location for that gsp
    4. Create an observer for that gsp and regime if it doesn't already exist
    5. Get the most recent observations for that location and observer,
    6. Remove any data points from our data that are already in the database
    7. Update location capacity based on the max capacity in this data
    8. Create new observations for the remaining data points

    :param data_df: DataFrame containing forecast data with required columns.
    """

    assert "target_datetime_utc" in data_df.columns
    assert "solar_generation_kw" in data_df.columns
    assert "gsp_id" in data_df.columns
    assert "regime" in data_df.columns

    # Initialize the Data Platform client
    channel = Channel(host=data_platform_host, port=data_platform_port)
    client = dp.DataPlatformDataServiceStub(channel)

    gsp_ids = data_df["gsp_id"].unique()

    # 0 Get all locations
    all_locations = await get_all_gsp_and_national_locations(client)

    # for each gsp
    for gsp_id in gsp_ids:
        logger.info(f"Saving GSP ID: {gsp_id} to Data Platform")

        # 1. Get only the data for that gsp
        gsp_data = data_df[data_df["gsp_id"] == gsp_id]

        # 2. Get the start and end timestamps from that data
        start_timestamp_utc = gsp_data["target_datetime_utc"].min()
        end_timestamp_utc = gsp_data["target_datetime_utc"].max()

        # 3. Get the location for that gsp
        location = all_locations.get(gsp_id)
        if location is None:
            logger.warning(f"No location found for GSP ID {gsp_id}, skipping.")
            continue
        location_uuid = location.location_uuid

        # 4. Create an observer for that gsp and regime if it doesn't already exist
        regime = gsp_data["regime"].iloc[0]
        name = f"PVLive-consumer-{regime}".lower()
        observer_request = dp.CreateObserverRequest(name=name)
        try:
            _ = await client.create_observer(observer_request)
        except Exception:
            logger.warning(f"Observer {name} probably already exists, so carrying on anyway.")

        # 5. Get the most recent observations for that location and observer
        recent_observations_request = dp.GetObservationsAsTimeseriesRequest(
            location_uuid=location_uuid,
            energy_source=dp.EnergySource.SOLAR,
            observer_name=name,
            time_window=dp.TimeWindow(
                start_timestamp_utc=start_timestamp_utc,
                end_timestamp_utc=end_timestamp_utc,
            ),
        )
        recent_observations = await client.get_observations_as_timeseries(
            recent_observations_request
        )

        # 6. Remove any data points from our data that are already in the database
        logger.debug(
            f"Found {len(recent_observations.values)} recent observations for location {location_uuid} and observer {name}."
        )
        data_in_database = {
            obs.timestamp_utc: obs.value_fraction for obs in recent_observations.values
        }
        gsp_data = gsp_data[~gsp_data["target_datetime_utc"].isin(data_in_database.keys())]

        if len(gsp_data) == 0:
            logger.debug(
                f"No new data to add for location {location_uuid} and observer {name}, for gsp id {gsp_id}."
            )
            continue

        # 7. Update location capacity based on the max capacity in this data
        new_max_capacity_watts = int(gsp_data["capacity_mwp"].max() * 1_000_000)
        max_capacity_watts_current = location.effective_capacity_watts
        if new_max_capacity_watts > max_capacity_watts_current:
            logger.info(
                f"Updating location {location_uuid} capacity from {max_capacity_watts_current}W to {new_max_capacity_watts}W."
            )

            update_location_request = dp.UpdateLocationCapacityRequest(
                location_uuid=location_uuid,
                energy_source=dp.EnergySource.SOLAR,
                valid_from_utc=pd.Timestamp.utcnow().to_pydatetime(),
                new_effective_capacity_watts=new_max_capacity_watts,
            )
            _ = await client.update_location_capacity(update_location_request)
        else:
            logger.debug(
                f"Location {location_uuid} capacity of {max_capacity_watts_current}W is sufficient; no update needed."
            )

        # 8. Create new observations for the remaining data points
        observation_values = []
        for _, row in gsp_data.iterrows():
            if row["capacity_mwp"] == 0:
                continue  # Skip entries with zero capacity to avoid division by zero

            value_fraction = row["solar_generation_kw"] / (row["capacity_mwp"] * 1000)
            effective_capacity_watts = int(row["capacity_mwp"] * 1_000_000)
            timestamp_utc = row["target_datetime_utc"].to_pydatetime()

            oberservation_value = dp.CreateObservationsRequestValue(
                timestamp_utc=timestamp_utc,
                value_fraction=value_fraction,
                effective_capacity_watts=effective_capacity_watts,
            )
            observation_values.append(oberservation_value)

        logger.debug(
            f"Adding {len(observation_values)} new observation values for location {location_uuid} and observer {name}."
        )
        if len(observation_values) > 0:
            observation_request = dp.CreateObservationsRequest(
                location_uuid=location_uuid,
                energy_source=dp.EnergySource.SOLAR,
                observer_name=name,
                values=observation_values,
            )
            _ = await client.create_observations(observation_request)

    channel.close()


async def get_all_gsp_and_national_locations(
    client: dp.DataPlatformDataServiceStub,
) -> dict[int, dp.ListLocationsResponseLocationSummary]:
    """Get all GSP and national locations for solar energy source"""

    all_locations = {}
    all_location_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.NATION,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    location_response = await client.list_locations(all_location_request)
    location = location_response.locations[0]
    all_locations = {0: location}

    all_location_gsp_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.GSP,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    location_response = await client.list_locations(all_location_gsp_request)
    for loc in location_response.locations:
        all_locations[loc.metadata.to_dict()["gsp_id"]["numberValue"]] = loc

    return all_locations

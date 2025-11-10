"""Functions to save to the Data-platform

https://github.com/openclimatefix/data-platform

"""

from loguru import logger
from datetime import datetime, timezone

from dp_sdk.ocf import dp
import pandas as pd


async def save_generation_to_data_platform(
    data_df: pd.DataFrame, client: dp.DataPlatformDataServiceStub | None = None
):
    """
    Save Generation data to the Data-platform.

    0. First we get all the locations

    Here's how we do it for each gsp
    1. Get all the locations
    2. Create an observer for that regime if it doesn't already exist
    3. For each gsp: Get only the data for that gsp
    4. Get the location for that gsp
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
    assert "capacity_mwp" in data_df.columns

    # 1. Get all locations (UK GSPs and National)
    all_gsp_and_national_locations = await get_all_gsp_and_national_locations(client)

    # 2. Create an observer for the regime if it doesn't already exist
    # Note that regime is either in-day or day-ahead,
    # and there should only be one regime per DataFrame
    regime = data_df["regime"].unique()
    assert len(regime) == 1, "DataFrame must contain only one regime type"
    regime = regime[0].lower().replace("-", "_")
    name = f"pvlive_consumer_{regime}"

    list_observers_request = dp.ListObserversRequest(observer_names_filter=[name])
    list_observers_response = await client.list_observers(list_observers_request)
    if len(list_observers_response.observers) == 0:
        observer_request = dp.CreateObserverRequest(name=name)
        _ = await client.create_observer(observer_request)

    # for each gsp
    gsp_ids = data_df["gsp_id"].unique()
    for gsp_id in gsp_ids:
        logger.info(f"Saving GSP ID: {gsp_id} to Data Platform")

        # 3. Get only the data for that gsp
        gsp_data = data_df[data_df["gsp_id"] == gsp_id]

        # 4. Get the location for that gsp
        location = all_gsp_and_national_locations.get(gsp_id)
        if location is None:
            logger.warning(f"No location found for GSP ID {gsp_id}, skipping.")
            continue
        location_uuid = location.location_uuid

        # 5. Get the most recent observations for that location and observer
        try:
            recent_observations_request = dp.GetLatestObservationRequest(
                location_uuid=location_uuid,
                energy_source=dp.EnergySource.SOLAR,
                observer_name=name,
            )
            recent_observation_response = await client.get_latest_observation(
                recent_observations_request
            )

            # 6. Remove any data points from our data that are already in the database
            last_datetime_utc = recent_observation_response.timestamp_utc
            gsp_data = gsp_data[
                gsp_data["target_datetime_utc"] > last_datetime_utc
            ]
        except Exception:
            logger.info(
                f"No existing observations for location {location_uuid} and observer {name}, adding all data."
            )


        if len(gsp_data) == 0:
            logger.debug(
                f"No new data to add for location {location_uuid} and observer {name}, for gsp id {gsp_id}."
            )
            continue

        # 7. Update location capacity based on the max capacity in this data
        new_max_capacity_watts = int(gsp_data["capacity_mwp"].max() * 1_000_000)
        max_capacity_watts_current = location.effective_capacity_watts
        if new_max_capacity_watts != max_capacity_watts_current:
            logger.info(
                f"Updating location {location_uuid} capacity from {max_capacity_watts_current}W to {new_max_capacity_watts}W."
            )

            update_location_request = dp.UpdateLocationCapacityRequest(
                location_uuid=location_uuid,
                energy_source=dp.EnergySource.SOLAR,
                valid_from_utc=datetime.now(tz=timezone.utc),
                new_effective_capacity_watts=new_max_capacity_watts,
            )
            _ = await client.update_location_capacity(update_location_request)

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

        if len(observation_values) > 0:

            logger.debug(
                f"Adding {len(observation_values)} new observation values for location {location_uuid} and observer {name}."
            )

            observation_request = dp.CreateObservationsRequest(
                location_uuid=location_uuid,
                energy_source=dp.EnergySource.SOLAR,
                observer_name=name,
                values=observation_values,
            )
            _ = await client.create_observations(observation_request)


async def get_all_gsp_and_national_locations(
    client: dp.DataPlatformDataServiceStub,
) -> dict[int, dp.ListLocationsResponseLocationSummary]:
    """Get all GSP and National locations for solar energy source"""

    all_locations = {}

    # National location
    all_location_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.NATION,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    location_response = await client.list_locations(all_location_request)
    all_uk_location = [loc for loc in location_response.locations if 'uk' in loc.location_name.lower()]
    if len(all_uk_location) == 1:
        all_locations[0] = all_uk_location[0]
    elif len(all_uk_location) == 0:
        raise Exception("No UK National location found.")
    else:
        raise Exception("Multiple UK National locations found.")

    # GSP locations
    all_location_gsp_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.GSP,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    location_response = await client.list_locations(all_location_gsp_request)
    for loc in location_response.locations:
        all_locations[loc.metadata.to_dict()["gsp_id"]["numberValue"]] = loc

    return all_locations

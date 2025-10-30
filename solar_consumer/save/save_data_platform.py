""" Functions to save to the Data-platform 

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
    Save forecast data to the Data-platform.

    :param data_df: DataFrame containing forecast data with required columns.
    """

    assert "target_datetime_utc" in data_df.columns
    assert "solar_generation_kw" in data_df.columns
    assert "gsp_id" in data_df.columns
    assert "regime" in data_df.columns

    # Initialize the Data Platform client
    channel = Channel(host=data_platform_host, port=data_platform_port)
    client = dp.DataPlatformServiceStub(channel)

    gsp_ids = data_df["gsp_id"].unique()

    # for each gsp
    for gsp_id in gsp_ids:
        gsp_data = data_df[data_df["gsp_id"] == gsp_id]

        regime = gsp_data["regime"].iloc[0]

        name = f"PVLive-consumer-{regime}".lower()

        # TODO get location
        print(name)

        observer_request = dp.CreateObserverRequest(name=name)
        try:
            _ = await client.create_observer(observer_request)
        except Exception as e:
            logger.warning(f"Observer {name} may already exist, but carrying on anyway.")
        # TODO get observer, if it already exists

        observation_values = []
        for _, row in gsp_data.iterrows():
            value_percent = int(row["solar_generation_kw"] / (row["capacity_mwp"]*1000) * 100) 

            # current have to add 1 to make it work
            # TODO remove this
            value_percent = value_percent+1

            oberservation_value = dp.CreateObservationsRequestValue(
                timestamp_utc=row["target_datetime_utc"].to_pydatetime(),
                value_percent=value_percent,
                effective_capacity_watts=int(row["capacity_mwp"] * 1_000_000),
            )
            observation_values.append(oberservation_value)

        # TODO update location uuid
        observation_request = dp.CreateObservationsRequest(location_uuid="0199f281-3721-7b66-a1c5-f5cf625088bf", 
                                     energy_source=dp.EnergySource.SOLAR,
                                     observer_name=name,
                                     values=observation_values,
                                     user_role="solar-consumer"
                                     )
        _ = await client.create_observations(observation_request)

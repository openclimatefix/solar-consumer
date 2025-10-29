""" Functions to save to the Data-platform 

https://github.com/openclimatefix/data-platform

"""

import os

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
    client = dp.DataPlatformDataServiceStub(channel)

    gsp_ids = data_df["gsp_id"].unique()

    # for each gsp
    for gsp_id in gsp_ids:
        gsp_data = data_df[data_df["gsp_id"] == gsp_id]

        regime = gsp_data["regime"].iloc[0]

        name = f"PVLive-consumer-{regime}"

        # TODO get location

        observer_request = dp.CreateObserverRequest(name=name)
        _ = await client.create_observer(observer_request)
        # TODO get observer, if it already exists

        observation_values = []
        for _, row in gsp_data.iterrows():
            oberservation_value = dp.CreateObservationsRequestValue(
                timestamp_utc=row["target_datetime_utc"].to_pydatetime(),
                value_percent=row["solar_generation_kw"],
                effective_capacity_watts=row["capacity_mwp"] * 1_000_000,
            )
            observation_values.append(oberservation_value)

        # TODO update location uuid
        observation_request = dp.CreateObservationsRequest(location_uuid="0199f281-3721-7b66-a1c5-f5cf625088bf", 
                                     energy_source=dp.EnergySource.ENERGY_SOURCE_SOLAR_PV,
                                     observer_name=name,
                                     values=observation_values,
                                     user_id="solar-consumer"
                                     )
        _ = await client.create_observations(observation_request)

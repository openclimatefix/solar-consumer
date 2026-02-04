"""Functions to save to the Data-platform

https://github.com/openclimatefix/data-platform

"""


import datetime
from dp_sdk.ocf import dp
import pandas as pd

import asyncio
import logging
from collections import defaultdict

import itertools

import betterproto

import numpy as np
from betterproto.lib.google.protobuf import Struct, Value

async def save_generation_to_data_platform(data_df: pd.DataFrame, client: dp.DataPlatformDataServiceStub) -> None:
    """
    Saves model data via the data platform.
    
    Incoming data is enriched with location information from the data platform. Anything with zero
    capacity, or without a corresponding entry in the data platform, is ignored.

    Data is joined via the gsp_id, which is a column in the incoming data, and has to be extracted
    from the metadata field in the data platform location data.
    """
    tasks: list[asyncio.Task] = []
    # 0. Create the observers required if they don't exist already
    required_observers = {"pvlive_in_day", "pvlive_day_after"}
    list_observer_request = dp.ListObserversRequest(
        observer_names_filter=list(required_observers),
    )
    list_observer_response = await client.list_observers(list_observer_request)
    create_observers = required_observers.difference({
        observer.observer_name for observer in list_observer_response.observers
    })
    for observer_name in create_observers:
        tasks.append(asyncio.create_task(
            client.create_observer(dp.CreateObserverRequest(name=observer_name))
        ))
    if len(tasks) > 0:
        logging.info("creating %d observers", len(tasks))
        create_observer_results = await asyncio.gather(*tasks, return_exceptions=True)
        for exc in filter(lambda x: isinstance(x, Exception), create_observer_results):
            raise exc

    # 1. Get the UK GSP locations, as well as national, and join to the incoming data.
    # * Fetched locations are assumed to be identifiable from any other locations returned by
    # * nature of "gsp_id" being in the metadata.
    # * Anything without a corresponding gsp_id in the incoming data is ignored.
    tasks = [
        asyncio.create_task(client.list_locations(
            dp.ListLocationsRequest(
                location_type_filter=loc_type,
                energy_source_filter=dp.EnergySource.SOLAR,
            )
        ))
        for loc_type in [dp.LocationType.GSP, dp.LocationType.NATION]
    ]
    list_results = await asyncio.gather(*tasks, return_exceptions=True)
    for exc in filter(lambda x: isinstance(x, Exception), list_results):
        raise exc

    joined_df = (
        # Convert and combine the location lists from the responses into a single DataFrame
        pd.DataFrame.from_dict(
            itertools.chain(*[
                r.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True)["locations"]
                for r in list_results]
            )
        )
        # Filter the returned locations to those with a gsp_id in the metadata; extract it
        .loc[lambda df: df["metadata"].apply(lambda x: "gsp_id" in x)]
        .assign(gsp_id=lambda df: df["metadata"].apply(lambda x: x["gsp_id"]["number_value"]))
        .set_index("gsp_id")
        # Join to the incoming data, ignoring 0 capacity locations
        .join(
            data_df.query("capacity_mwp>0").set_index("gsp_id"), on="gsp_id", how="inner", lsuffix="_loc"
        )
        # Make types and units uniform between the two sources of data
        .assign(new_effective_capacity_watts=lambda df: (df["capacity_mwp"] * 1e6).astype(int))
        .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
    )

    logging.info("handling data for %d matched locations", joined_df["location_uuid"].nunique())

    # 2. Generate the UpdateLocationCapacityRequest objects from the DataFrame.
    # * Should only occur when the incoming data has a different capacity to that returned by the
    # * data platform. The most recent value for a given location is the one that is used.
    #
    # TODO, we've put in a limit of relative tolerance of 2% here to avoid tiny changes triggering updates,
    # This is references in https://github.com/openclimatefix/data-platform/issues/71
    joined_df['capacity_change'] = ((joined_df["effective_capacity_watts"].astype(float))/(joined_df["new_effective_capacity_watts"].astype(float))).abs()
    updates_df = (
        joined_df
        .loc[
            lambda df: ~np.isclose(df["capacity_change"], 1.0, rtol=0.02)
        ]
        .sort_values(by='target_datetime_utc', ascending=False)
        .groupby(level=0)
        .head(1)
        .sort_index()
    )
    tasks = []
    for lid, t, new_cap in zip(
        updates_df["location_uuid"],
        updates_df["target_datetime_utc"],
        updates_df["new_effective_capacity_watts"],
    ):
        req = dp.UpdateLocationRequest(
            location_uuid=lid,
            energy_source=dp.EnergySource.SOLAR,
            new_effective_capacity_watts=new_cap,
            valid_from_utc=t,
        )
        tasks.append(asyncio.create_task(client.update_location(req)))
    
    if len(tasks) > 0:
        logging.info("updating %d location capacities", len(tasks))
        update_results = await asyncio.gather(*tasks, return_exceptions=True)
        for exc in filter(lambda x: isinstance(x, Exception), update_results):
            raise exc

    # 3. Generate the CreateObservationRequest objects from the DataFrame.
    # * The observer is assumed to exist already, and only one regime is assumed to be present
    # * within the DataFrame.
    observations_by_loc: dict[str, list[dp.CreateObservationsRequestValue]] = defaultdict(list)
    for lid, t, val in zip(
        joined_df["location_uuid"],
        joined_df["target_datetime_utc"],
        (joined_df["solar_generation_kw"] * 1000).astype(int),
    ):
        observations_by_loc[lid].append(dp.CreateObservationsRequestValue(
            timestamp_utc=t,
            value_watts=val
        ))
    regime: str = data_df["regime"].values[0]
    tasks = [
        asyncio.create_task(client.create_observations(
            dp.CreateObservationsRequest(
                location_uuid=lid,
                energy_source=dp.EnergySource.SOLAR,
                observer_name=f"pvlive_{regime.replace('-', '_')}",
                values=vals,
            ),
        ))
        for lid, vals in observations_by_loc.items()
    ]

    if len(tasks) > 0:
        logging.info("creating observations for %d locations", len(tasks))
        create_results = await asyncio.gather(*tasks)
        for exc in filter(lambda x: isinstance(x, Exception), create_results):
            raise exc


async def save_be_generation_to_data_platform(
    data_df: pd.DataFrame,
    client: dp.DataPlatformDataServiceStub,
) -> None:
    """
    Saves Belgian solar generation data via the data platform.

    Incoming data is joined to Data Platform locations using a "region" key
    from location metadata (preferred) or location name.
    """
    if data_df.empty:
        logging.info("No Belgian generation data to save.")
        return
    tasks: list[asyncio.Task] = []
    required_observers = {"elia_be"}

    list_observer_request = dp.ListObserversRequest(
        observer_names_filter=list(required_observers),
    )
    list_observer_response = await client.list_observers(list_observer_request)
    create_observers = required_observers.difference({
        observer.observer_name for observer in list_observer_response.observers
    })
    for observer_name in create_observers:
        tasks.append(
            asyncio.create_task(
                client.create_observer(dp.CreateObserverRequest(name=observer_name))
            )
        )
    if len(tasks) > 0:
        logging.info("creating %d observers", len(tasks))
        create_observer_results = await asyncio.gather(*tasks, return_exceptions=True)
        for exc in filter(lambda x: isinstance(x, Exception), create_observer_results):
            raise exc

    list_locations_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.NATION,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    list_locations_response = await client.list_locations(list_locations_request)

    locations_data = list_locations_response.to_dict(
        casing=betterproto.Casing.SNAKE,
        include_default_values=True,
    ).get("locations", [])

    if not locations_data:
        default_locations = [
            {"name": "Belgium", "region": "Belgium", "latitude": "50.85", "longitude": "4.35"},
            {"name": "Flanders", "region": "Flanders", "latitude": "51.00", "longitude": "4.46"},
            {"name": "Wallonia", "region": "Wallonia", "latitude": "50.50", "longitude": "4.70"},
            {"name": "Brussels", "region": "Brussels", "latitude": "50.85", "longitude": "4.35"},
        ]

        for location in default_locations:
            location_name = f"be_{location['region'].lower().replace(' ', '_')}"
            create_location_request = dp.CreateLocationRequest(
                location_name=location_name,
                energy_source=dp.EnergySource.SOLAR,
                location_type=dp.LocationType.NATION,
                geometry_wkt="POINT({} {})".format(
                    location["longitude"], location["latitude"]
                ),
                effective_capacity_watts=100_000_000_000,
                metadata=Struct(
                    fields={"region": Value(string_value=location["region"])}
                ),
                valid_from_utc=datetime.datetime(
                    2020, 1, 1, tzinfo=datetime.timezone.utc
                ),
            )
            await client.create_location(create_location_request)
        logging.warning(
            "No BE locations found in data platform. Creating default locations."
        )

        list_locations_response = await client.list_locations(list_locations_request)
        locations_data = list_locations_response.to_dict(
            casing=betterproto.Casing.SNAKE,
            include_default_values=True,
        ).get("locations", [])

    locations_df = pd.DataFrame.from_dict(locations_data)

    data_df = data_df.copy()
    data_df["region_key"] = data_df["region"].astype(str).str.strip().str.lower()

    joined_df = (
        locations_df
        .assign(
            region_key=lambda df: df["metadata"].apply(
                lambda x: (
                    x.get("region", {}).get("string_value")
                    if isinstance(x, dict) and "region" in x
                    else None
                )
            )
        )
        .assign(
            region_key=lambda df: df["region_key"].fillna(df["location_name"])
        )
        .assign(region_key=lambda df: df["region_key"].astype(str).str.strip().str.lower())
        .set_index("region_key")
        .join(
            data_df.query("capacity_mwp>0").set_index("region_key"),
            on="region_key",
            how="inner",
            lsuffix="_loc",
        )
        .assign(new_effective_capacity_watts=lambda df: (df["capacity_mwp"] * 1e6).astype(int))
        .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
    )

    if joined_df.empty:
        logging.warning(
            "No matching BE locations found for incoming data. "
            "Ensure BE locations exist with region metadata matching regions: %s",
            sorted(data_df["region"].unique().tolist()),
        )
        return

    logging.info(
        "handling BE data for %d matched locations",
        joined_df["location_uuid"].nunique(),
    )

    joined_df["capacity_change"] = (
        (joined_df["effective_capacity_watts"].astype(float))
        / (joined_df["new_effective_capacity_watts"].astype(float))
    ).abs()
    updates_df = (
        joined_df
        .loc[lambda df: ~np.isclose(df["capacity_change"], 1.0, rtol=0.02)]
        .sort_values(by="target_datetime_utc", ascending=False)
        .groupby(level=0)
        .head(1)
        .sort_index()
    )

    tasks = []
    for lid, t, new_cap in zip(
        updates_df["location_uuid"],
        updates_df["target_datetime_utc"],
        updates_df["new_effective_capacity_watts"],
    ):
        req = dp.UpdateLocationRequest(
            location_uuid=lid,
            energy_source=dp.EnergySource.SOLAR,
            new_effective_capacity_watts=new_cap,
            valid_from_utc=t,
        )
        tasks.append(asyncio.create_task(client.update_location(req)))

    if len(tasks) > 0:
        logging.info("updating %d BE location capacities", len(tasks))
        update_results = await asyncio.gather(*tasks, return_exceptions=True)
        for exc in filter(lambda x: isinstance(x, Exception), update_results):
            raise exc

    observations_by_loc: dict[str, list[dp.CreateObservationsRequestValue]] = defaultdict(list)
    for lid, t, val in zip(
        joined_df["location_uuid"],
        joined_df["target_datetime_utc"],
        (joined_df["solar_generation_kw"] * 1000).astype(int),
    ):
        observations_by_loc[lid].append(
            dp.CreateObservationsRequestValue(timestamp_utc=t, value_watts=val)
        )

    tasks = [
        asyncio.create_task(
            client.create_observations(
                dp.CreateObservationsRequest(
                    location_uuid=lid,
                    energy_source=dp.EnergySource.SOLAR,
                    observer_name="elia_be",
                    values=vals,
                ),
            )
        )
        for lid, vals in observations_by_loc.items()
    ]

    if len(tasks) > 0:
        logging.info("creating observations for %d BE locations", len(tasks))
        create_results = await asyncio.gather(*tasks)
        for exc in filter(lambda x: isinstance(x, Exception), create_results):
            raise exc
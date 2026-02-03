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

async def save_nl_generation_to_data_platform(data_df: pd.DataFrame, client: dp.DataPlatformDataServiceStub) -> None:
    """
    Saves NL solar generation data via the data platform.
    """
    tasks: list[asyncio.Task] = []
    required_observers = {"nednl"}
    
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

    # 1. Get the NL locations, and join to the incoming data.
    # * Fetched locations are assumed to be identifiable from any other locations returned by
    # * nature of "region_id" being in the metadata.
    # * Anything without a corresponding region_id in the incoming data is ignored.
    list_locations_request = dp.ListLocationsRequest(
        location_type_filter=dp.LocationType.NATION,
        energy_source_filter=dp.EnergySource.SOLAR,
    )
    list_locations_response = await client.list_locations(list_locations_request)

    # Convert response to DataFrame
    locations_data = list_locations_response.to_dict(
        casing=betterproto.Casing.SNAKE,
        include_default_values=True,
    ).get("locations", [])

    if not locations_data:
        #TODO: load from csv
        locations=[dict(name="nl_national", latitude="52.13", longitude="5.29", region_id=0), dict(name="nl_groningen", latitude="53.22", longitude="6.74", region_id=1)]
        for location in locations:
            location_name= location["name"]
            create_location_request= dp.CreateLocationRequest(
                location_name=location_name,
                energy_source=dp.EnergySource.SOLAR,
                location_type=dp.LocationType.NATION,
                geometry_wkt="POINT({} {})".format(location["longitude"], location["latitude"]),
                effective_capacity_watts=100_000_000_000,
                metadata = Struct(fields={"region_id": Value(number_value=location["region_id"])}),
                valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
                )
            await client.create_location(create_location_request)
        logging.warning("No NL locations found in data platform. Creating new locations.")

    locations_df = pd.DataFrame.from_dict(locations_data)
    print("################")
    print(locations_df['effective_capacity_watts'])
    print("################")
    # Prepare incoming data: map region_id to region_id (int)
    data_df = data_df.copy()
    data_df["region_id"] = data_df["region_id"].astype(int)

    joined_df = (
        locations_df
        .assign(region_id=lambda df: df["metadata"].apply(lambda x:x["region_id"]["number_value"]))
        .set_index("region_id")
        # Join to the incoming data, ignoring 0 capacity locations
        .join(
            data_df.query("capacity_kw>0").set_index("region_id"), on="region_id", how="inner", lsuffix="_loc"
        )
        # Make types and units uniform between the two sources of data
        .assign(new_effective_capacity_watts=lambda df: (df["capacity_kw"] * 1000).astype(int))
        .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
    )
    if joined_df.empty:
        logging.warning("No matching NL locations found for the incoming data. "
                       "Ensure NL locations exist in the data platform with region_id metadata matching region_ids: %s",
                       data_df["region_id"].unique().tolist())
        return

    logging.info("handling NL data for %d matched locations", joined_df["location_uuid"].nunique())
    joined_df['capacity_change'] = (
        (joined_df["effective_capacity_watts"].astype(float)) /
        (joined_df["new_effective_capacity_watts"].astype(float))
    ).abs()
    updates_df = (
        joined_df
        .loc[lambda df: ~np.isclose(df["capacity_change"], 1.0, rtol=0.02)]
        .sort_values(by='target_datetime_utc', ascending=False)
        .groupby(level=0)
        .head(1)
        .sort_index()
    )
    # print("################")
    # # print(joined_df["capacity_change"])
    # print(joined_df["effective_capacity_watts"])
    # print("################")
    # print(joined_df["new_effective_capacity_watts"])
    # print("################")
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
    # print("################")
    # print(updates_df)
    # print("################")
    if len(tasks) > 0:
        logging.info("updating %d NL location capacities", len(tasks))
        update_results = await asyncio.gather(*tasks, return_exceptions=True)
        for exc in filter(lambda x: isinstance(x, Exception), update_results):
            # raise exc
            pass
    # 3. Generate the CreateObservationRequest objects from the DataFrame.
    # * The observer "nednl" is used for all NL generation data.
    observations_by_loc: dict[str, list[dp.CreateObservationsRequestValue]] = defaultdict(list)
    for lid, t, val, eff_cap in zip(
        joined_df["location_uuid"],
        joined_df["target_datetime_utc"],
        (joined_df["solar_generation_kw"] * 1000).astype(int),
        joined_df["effective_capacity_watts"],
    ):
        observations_by_loc[lid].append(dp.CreateObservationsRequestValue(
            timestamp_utc=t,
            value_watts=val
        ))
        print("################")
        print(lid, t, val, eff_cap, int(val)<int(eff_cap))
        print("################")
    # print(observations_by_loc.keys())
    # print("################")
    tasks = [
        asyncio.create_task(client.create_observations(
            dp.CreateObservationsRequest(
                location_uuid=lid,
                energy_source=dp.EnergySource.SOLAR,
                observer_name="nednl",
                values=vals,
            ),
        ))
        for lid, vals in observations_by_loc.items()
    ]

    if len(tasks) > 0:
        logging.info("creating observations for %d NL locations", len(tasks))
        create_results = await asyncio.gather(*tasks)
        for exc in filter(lambda x: isinstance(x, Exception), create_results):
            raise exc



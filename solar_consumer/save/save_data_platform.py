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
from pathlib import Path


async def save_generation_to_data_platform(
    data_df: pd.DataFrame, client: dp.DataPlatformDataServiceStub, country: str = "gbr_gb"
) -> None:
    """
    Saves model data via the data platform.

    Incoming data is enriched with location information from the data platform. Anything with zero
    capacity, or without a corresponding entry in the data platform, is ignored.

    For GBR_GB: Data is joined via the gsp_id, which is a column in the incoming data, and has to be
    extracted from the metadata field in the data platform location data.

    For NLD: Data is joined via the region_id.

    Args:
        data_df: DataFrame containing the generation data
        client: Data platform client stub
        country: Country identifier ('gbr_gb' or 'nld')
    """
    tasks: list[asyncio.Task] = []

    # 0. Create the observers required if they don't exist already
    if country == "nld":
        required_observers = {"nednl"}
        id_key = "region_id"
        capacity_col = "capacity_kw"
        capacity_multiplier = 1000
    else:  # gbr_gb
        required_observers = {"pvlive_in_day", "pvlive_day_after"}
        id_key = "gsp_id"
        capacity_col = "capacity_mwp"
        capacity_multiplier = 1e6

    list_observer_request = dp.ListObserversRequest(
        observer_names_filter=list(required_observers),
    )
    list_observer_response = await client.list_observers(list_observer_request)
    create_observers = required_observers.difference(
        {observer.observer_name for observer in list_observer_response.observers}
    )
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

    # 1. Get locations and join to the incoming data.
    if country == "nld":
        # Get NL locations (NATION only)
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
            # Load NL locations from CSV
            csv_path = Path(__file__).parent.parent / "data" / "nl_locations.csv"
            if not csv_path.exists():
                raise FileNotFoundError(f"NL locations CSV not found at {csv_path}")
            locations_df_csv = pd.read_csv(csv_path)
            locations = locations_df_csv.to_dict(orient="records")
            for loc in locations:
                location_name = loc["name"]
                create_location_request = dp.CreateLocationRequest(
                    location_name=location_name,
                    energy_source=dp.EnergySource.SOLAR,
                    location_type=dp.LocationType.NATION,
                    geometry_wkt="POINT({} {})".format(loc["longitude"], loc["latitude"]),
                    effective_capacity_watts=100_000_000_000,
                    metadata=Struct(fields={"region_id": Value(number_value=loc["region_id"])}),
                    valid_from_utc=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
                )
                await client.create_location(create_location_request)
            logging.warning("No NL locations found in data platform. Created new locations.")

            # Re-fetch locations after creating them
            list_locations_response = await client.list_locations(list_locations_request)
            locations_data = list_locations_response.to_dict(
                casing=betterproto.Casing.SNAKE,
                include_default_values=True,
            ).get("locations", [])

        locations_df = pd.DataFrame.from_dict(locations_data)

        # Prepare incoming data: map region_id to int
        data_df = data_df.copy()
        data_df["region_id"] = data_df["region_id"].astype(int)

        joined_df = (
            locations_df.assign(
                region_id=lambda df: df["metadata"].apply(lambda x: x["region_id"]["number_value"])
            )
            .set_index("region_id")
            .join(
                data_df.query(f"{capacity_col}>0").set_index("region_id"),
                on="region_id",
                how="inner",
                lsuffix="_loc",
            )
            .assign(
                new_effective_capacity_watts=lambda df: (
                    df[capacity_col] * capacity_multiplier
                ).astype(int)
            )
            .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
        )
    else:  # gbr_gb
        # Get UK GSP locations, as well as national
        tasks = [
            asyncio.create_task(
                client.list_locations(
                    dp.ListLocationsRequest(
                        location_type_filter=loc_type,
                        energy_source_filter=dp.EnergySource.SOLAR,
                    )
                )
            )
            for loc_type in [dp.LocationType.GSP, dp.LocationType.NATION]
        ]
        list_results = await asyncio.gather(*tasks, return_exceptions=True)
        for exc in filter(lambda x: isinstance(x, Exception), list_results):
            raise exc

        joined_df = (
            pd.DataFrame.from_dict(
                itertools.chain(
                    *[
                        r.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True)[
                            "locations"
                        ]
                        for r in list_results
                    ]
                )
            )
            .loc[lambda df: df["metadata"].apply(lambda x: "gsp_id" in x)]
            .assign(gsp_id=lambda df: df["metadata"].apply(lambda x: x["gsp_id"]["number_value"]))
            .set_index("gsp_id")
            .join(
                data_df.query(f"{capacity_col}>0").set_index("gsp_id"),
                on="gsp_id",
                how="inner",
                lsuffix="_loc",
            )
            .assign(
                new_effective_capacity_watts=lambda df: (
                    df[capacity_col] * capacity_multiplier
                ).astype(int)
            )
            .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
        )

    if joined_df.empty:
        logging.warning(
            "No matching %s locations found for the incoming data. "
            "Ensure locations exist in the data platform with %s metadata matching %s: %s",
            country.upper(),
            id_key,
            id_key,
            data_df[id_key].unique().tolist() if id_key in data_df.columns else "N/A",
        )
        return

    logging.info(
        "handling %s data for %d matched locations",
        country.upper(),
        joined_df["location_uuid"].nunique(),
    )

    # 2. Generate the UpdateLocationCapacityRequest objects from the DataFrame.
    # * Should only occur when the incoming data has a different capacity to that returned by the
    # * data platform. The most recent value for a given location is the one that is used.
    #
    # TODO, we've put in a limit of relative tolerance of 2% here to avoid tiny changes triggering updates,
    # This is references in https://github.com/openclimatefix/data-platform/issues/71
    joined_df["capacity_change"] = (
        (joined_df["effective_capacity_watts"].astype(float))
        / (joined_df["new_effective_capacity_watts"].astype(float))
    ).abs()
    updates_df = (
        joined_df.loc[lambda df: ~np.isclose(df["capacity_change"], 1.0, rtol=0.02)]
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
        logging.info("updating %d %s location capacities", len(tasks), country.upper())
        update_results = await asyncio.gather(*tasks, return_exceptions=True)
        for exc in filter(lambda x: isinstance(x, Exception), update_results):
            if country != "nld":  # NL was previously ignoring these exceptions
                raise exc

    # 3. Generate the CreateObservationRequest objects from the DataFrame.
    observations_by_loc: dict[str, list[dp.CreateObservationsRequestValue]] = defaultdict(list)
    for lid, t, val in zip(
        joined_df["location_uuid"],
        joined_df["target_datetime_utc"],
        (joined_df["solar_generation_kw"] * 1000).astype(int),
    ):
        observations_by_loc[lid].append(
            dp.CreateObservationsRequestValue(timestamp_utc=t, value_watts=val)
        )

    # Determine observer name based on country
    if country == "nld":
        observer_name = "nednl"
    else:  # gbr_gb
        regime: str = data_df["regime"].values[0]
        observer_name = f"pvlive_{regime.replace('-', '_')}"

    tasks = [
        asyncio.create_task(
            client.create_observations(
                dp.CreateObservationsRequest(
                    location_uuid=lid,
                    energy_source=dp.EnergySource.SOLAR,
                    observer_name=observer_name,
                    values=vals,
                ),
            )
        )
        for lid, vals in observations_by_loc.items()
    ]

    if len(tasks) > 0:
        logging.info("creating observations for %d %s locations", len(tasks), country.upper())
        create_results = await asyncio.gather(*tasks)
        for exc in filter(lambda x: isinstance(x, Exception), create_results):
            raise exc

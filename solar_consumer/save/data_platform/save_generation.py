"""Functions to save generation to the Data-platform

https://github.com/openclimatefix/data-platform

"""

import asyncio
import datetime
from collections import defaultdict

import pandas as pd
from betterproto.lib.google.protobuf import Struct, Value
from loguru import logger
from ocf import dp

from solar_consumer.save.data_platform.utils import (
    _execute_async_tasks,
    _extract_metadata_value,
    _get_country_config,
    _list_locations,
    format_metadata_from_dict,
)

# the data platform accepts at most 5000 updates per batch
MAX_BATCH_UPDATE_SIZE = 5000


async def _batch_update_location_capacities(
    client: dp.DataPlatformDataServiceStub,
    updates_by_key: dict[tuple, list[dp.BatchUpdateLocationCapacityRequestUpdate]],
) -> None:
    """Send capacity updates in batches, one batch per (energy source, valid from time).

    Batches are sent one after another, so the data platform isn't overloaded, and a location
    with several energy sources, e.g. RUVNL's solar and wind, isn't updated concurrently.
    """
    for (energy_source, valid_from_utc), updates in updates_by_key.items():
        for i in range(0, len(updates), MAX_BATCH_UPDATE_SIZE):
            await client.batch_update_location_capacity(
                dp.BatchUpdateLocationCapacityRequest(
                    energy_source=energy_source,
                    updates=updates[i : i + MAX_BATCH_UPDATE_SIZE],
                    valid_from_utc=valid_from_utc,
                )
            )


async def _filter_existing_observations(
    joined_df: pd.DataFrame,
    client: dp.DataPlatformDataServiceStub,
    observer_name: str,
) -> pd.DataFrame:
    """Filter out observations that already exist in the data platform."""
    if joined_df.empty:
        return joined_df

    # Get the locations
    location_sources = joined_df[["location_uuid", "energy_source"]].drop_duplicates()
    
    # Get the min and max timestamps
    min_timestamp = joined_df["target_datetime_utc"].min()
    max_timestamp = datetime.datetime.now(datetime.UTC)

    # Read generation values from data platform, in parallel for all locations.
    read_observations_tasks = []
    for lid, es in location_sources.itertuples(index=False):
        req = dp.GetObservationsAsTimeseriesRequest(
            location_uuid=lid,
            observer_name=observer_name,
            energy_source=dp.EnergySource[es],
            time_window=dp.TimeWindow(
                start_timestamp_utc=min_timestamp,
                end_timestamp_utc=max_timestamp
            )
        )
        read_observations_tasks.append(asyncio.create_task(client.get_observations_as_timeseries(req)))

    if len(read_observations_tasks) > 0:
        logger.info(f"reading observations for {len(read_observations_tasks)}  locations")
        await _execute_async_tasks(read_observations_tasks, ignore_exceptions=True)

    # Compare timestamps already in the data platform
    # (existing_pairs built below, together with the filter)

    # Build a set of (location_uuid, energy_source, timestamp) rows that already exist in the
    # data platform. We must match on all three dimensions so that a timestamp saved for one
    # location, or for one energy source of it, does not accidentally suppress the same
    # timestamp elsewhere.
    existing_rows = []
    for (lid, es), task in zip(location_sources.itertuples(index=False), read_observations_tasks):
        try:
            for obs in task.result().values:
                existing_rows.append(
                    {
                        "location_uuid": lid, 
                        "energy_source": es, 
                        "target_datetime_utc": obs.timestamp_utc
                    }
                )
        # best-effort read: any RPC failure just means we skip this location
        except Exception as e:  # noqa: BLE001
            logger.error(f"Failed to read observations for location_uuid {lid} ({es}): {e}")

    # if any (location, source, timestamp) pairs already in the data-platform, remove from data in app
    if existing_rows:
        existing_pairs_df = pd.DataFrame(existing_rows).assign(
            target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"])
        )
        existing_pairs_df["_exists"] = True

        merged = joined_df.merge(
            existing_pairs_df,
            on=["location_uuid", "energy_source", "target_datetime_utc"],
            how="left",
        )
        idx = merged["_exists"].fillna(False)
        if idx.any():
            duplicate_location_uuids = merged.loc[idx, "location_uuid"].unique()
            logger.warning(
                f"Found {idx.sum()} values already existing in data platform "
                f"for location_uuid {duplicate_location_uuids}. "
                "These values will be dropped."
            )
            joined_df = merged[~idx].drop(columns=["_exists"])

    return joined_df


async def save_generation_to_data_platform(
    data_df: pd.DataFrame, client: dp.DataPlatformDataServiceStub, config_name: str = "gb"
) -> None:
    """
    Saves model data via the data platform.

    Incoming data is enriched with location information from the data platform. Anything with zero
    capacity, or without a corresponding entry in the data platform, is ignored.

    For GB: Data is joined via the gsp_id, which is a column in the incoming data, and has to be
    extracted from the metadata field in the data platform location data.

    For NL: Data is joined via the region_id.

    For BE and DE: Data is joined via the region (string-based matching).

    Args:
        data_df: DataFrame containing the generation data
        client: Data platform client stub
        config_name: Country identifier ('gb', 'nl', 'be', 'de' or 'ind_rajasthan')
    """
    tasks: list[asyncio.Task] = []
    config = _get_country_config(config_name)
    country = config['country']
    
    id_key = config["id_key"]
    # capacity_col and capacity_multiplier are no longer needed as we standardized on capacity_kw
    metadata_type = config["metadata_type"]
    rolling_capacity = config.get("rolling_capacity", False)

    # dropping the source capacity makes the max generation fallback below kick in
    if rolling_capacity:
        data_df = data_df.drop(columns="capacity_kw", errors="ignore")
    
    # Determine required observers
    # If observer_name is in config (NL/BE), use it as the single required observer
    # If not (GB), use the explicit list from config
    observer_name_config = config["observer_name"]
    if observer_name_config:
        required_observers = {observer_name_config}
    else:
        required_observers = config["required_observers"]

    # 0. Create the observers required if they don't exist already

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
        logger.info(f"creating {len(tasks)} observers")
        await _execute_async_tasks(tasks)

    # 1. Get locations and join to the incoming data.
    locations_data = await _list_locations(client, config["location_type"], country=country)

    # Convert locations to DataFrame
    locations_df = pd.DataFrame.from_dict(locations_data)

    # Prepare incoming data copy
    data_df = data_df.copy()

    # If the source data has no capacity, define it as the max generation so capacity_kw is
    # always present.
    if "capacity_kw" not in data_df.columns:
        if data_df.empty:
            data_df["capacity_kw"] = pd.Series(dtype=float)
        else:
            capacity_key = "energy_type" if country == "ind_rajasthan" else id_key
            if capacity_key in data_df.columns:
                data_df["capacity_kw"] = data_df.groupby(capacity_key)["solar_generation_kw"].transform("max")
            else:
                data_df["capacity_kw"] = data_df["solar_generation_kw"].max()
            logger.info("No capacity info found, so using max generation")

    # Extract metadata and create join key based on country
    if country == "ind_rajasthan":
        data_df["name"] = "ruvnl"
        data_df["join_key"] = data_df["name"] + "_" + data_df["energy_type"].astype(str).str.lower()

        if not locations_df.empty:
            locations_df = locations_df.assign(
                join_key=lambda df: df["location_name"].astype(str).str.strip().str.lower()
                + "_"
                + df["energy_source"].astype(str).str.strip().str.lower()
            )

    elif country in ["be", "de"]:
        # BE and DE use string matching with normalization
        if not data_df.empty and id_key in data_df.columns:
            data_df["join_key"] = data_df[id_key].astype(str).str.strip().str.lower()
        else:
            data_df["join_key"] = ""

        if not locations_df.empty:
            locations_df = locations_df.assign(
                join_key=lambda df: df["metadata"].apply(
                    lambda x: _extract_metadata_value(x, id_key, metadata_type)
                )
            ).assign(
                join_key=lambda df: df["join_key"].fillna(df["location_name"])
            ).assign(
                join_key=lambda df: df["join_key"].astype(str).str.strip().str.lower()
            )
    else:
        # NL and GB use numeric matching
        data_df["join_key"] = data_df[id_key]
        
        if not locations_df.empty:
            locations_df = (
                locations_df
                .loc[lambda df: df["metadata"].apply(lambda x: id_key in x)]
                .assign(
                    join_key=lambda df: df["metadata"].apply(
                        lambda x: _extract_metadata_value(x, id_key, metadata_type)
                    )
                )
            )
    
    # Common join logic for all countries
    if not (locations_df.empty or data_df.empty):
        joined_df = (
            locations_df
            .set_index("join_key")
            .join(
                data_df.query("capacity_kw!=0").set_index("join_key"),
                on="join_key",
                how="inner",
                lsuffix="_loc",
            )
            .assign(
                new_effective_capacity_watts=lambda df: (
                    df["capacity_kw"] * 1000
                )
            )
            .assign(target_datetime_utc=lambda df: pd.to_datetime(df["target_datetime_utc"]))
        )
    else:
        joined_df = pd.DataFrame()

    if joined_df.empty:
        # Check if the input data was empty or had no valid capacity data
        has_valid_capacity_data = not data_df.empty and (data_df["capacity_kw"] != 0).any()
        
        if data_df.empty or not has_valid_capacity_data:
            # Empty input or all zero-capacity data - this is expected, return silently
            return
        
        # Non-empty data with capacity but no matching locations - this is unexpected
        incoming_ids = data_df[id_key].unique().tolist() if id_key in data_df.columns else []
        raise ValueError(
            f"No matching {country.upper()} locations found for the incoming data. "
            f"Expected locations to exist in the data platform with {id_key} metadata "
            f"matching the following {id_key} values: {incoming_ids}. "
            f"This is unexpected - locations should already exist in the data platform."
        )

    logger.info(
        f"handling {country.upper()} data "
        f"for {joined_df['location_uuid'].nunique()} matched locations",
    )

    # 2. Generate the UpdateLocationCapacityRequest objects from the DataFrame.
    # * Should only occur when the incoming data has a different capacity to that returned by the
    # * data platform. The most recent value for a given location is the one that is used.
    updates_df = get_update_capacity_df(joined_df, rolling_capacity=rolling_capacity)

    # Grouped by energy source and valid from time, as each batch request shares these
    updates_by_key: dict[tuple, list[dp.BatchUpdateLocationCapacityRequestUpdate]] = (
        defaultdict(list)
    )
    for row in updates_df.itertuples():
        lid = row.location_uuid
        t = row.target_datetime_utc
        new_cap = row.new_effective_capacity_watts
        old_cap = row.effective_capacity_watts
        metadata = row.metadata

        gsp_id_val = (
            _extract_metadata_value(row.metadata, "gsp_id", "number")
            if isinstance(row.metadata, dict) else "?"
        )
        old_cap_kw = round(old_cap / 1000, 1) if old_cap else 0
        new_cap_kw = round(new_cap / 1000, 1) if new_cap else 0
        logger.info(
            f"UpdateLocation | loc={lid} gsp_id={gsp_id_val} | "
            f"capacity: {old_cap_kw} kW → {new_cap_kw} kW (at {t})"
        )

        # this is specific to GB consumer at the moment
        if "capacity_no_degradation_kw" in updates_df.columns:
            metadata = format_metadata_from_dict(metadata=row.metadata)
            metadata["capacity_no_degradation_kw"] = Value(number_value=int(row.capacity_no_degradation_kw))
            metadata = Struct(fields=metadata)
        else:
            metadata = None

        energy_source = dp.EnergySource[row.energy_source] if hasattr(row, 'energy_source') else dp.EnergySource.SOLAR
        update = dp.BatchUpdateLocationCapacityRequestUpdate(
            location_uuid=lid,
            new_effective_capacity_watts=int(new_cap),
            new_metadata=metadata,
        )
        updates_by_key[(energy_source, t)].append(update)

    if updates_by_key:
        update_count = sum(len(updates) for updates in updates_by_key.values())
        logger.info(f"updating {update_count} {country.upper()} location capacities")
        await _batch_update_location_capacities(client, updates_by_key)

    # Determine observer name based on country
    observer_name = config["observer_name"]
    if observer_name is None:  # GB needs regime from data
        regime: str = data_df["regime"].values[0]
        observer_name = f"pvlive_{regime.replace('-', '_')}"

    # 3. Generate the CreateObservationRequest objects from the DataFrame.

    # lets check none of the values are above 109% of the capacity
    # the limit is 110% but sometimes there are some rounding errors
    # if they are lets remove them
    idx = joined_df["solar_generation_kw"] > (joined_df["capacity_kw"] * 1.09)
    if idx.any():
        location_uuids = joined_df.loc[idx, "location_uuid"].unique()
        logger.warning(f"Found {idx.sum()} values above 109% of capacity \
                        for location_uuid {location_uuids}. \
                        These values will be dropped.")
        joined_df = joined_df[~idx]

    # Filter out observations that already exist in the data platform
    joined_df = await _filter_existing_observations(
        joined_df=joined_df,
        client=client,
        observer_name=observer_name,
    )


    observations_by_source: dict[tuple[str, str], list[dp.CreateObservationsRequestValue]] = defaultdict(list)
    energy_source_by_loc: dict[str, dp.EnergySource] = {}
    for lid, t, val, es in zip(
        joined_df["location_uuid"],
        joined_df["target_datetime_utc"],
        (joined_df["solar_generation_kw"] * 1000).astype(int),
        joined_df["energy_source"],
    ):
        observations_by_source[(lid, es)].append(
            dp.CreateObservationsRequestValue(timestamp_utc=t, value_watts=int(val))
        )
        energy_source_by_loc[lid] = dp.EnergySource[es]

    

    tasks = [
        asyncio.create_task(
            client.create_observations(
                dp.CreateObservationsRequest(
                    location_uuid=lid,
                    energy_source=dp.EnergySource[es],
                    observer_name=observer_name,
                    values=vals,
                ),
            )
        )
        for (lid, es), vals in observations_by_source.items()
    ]

    if len(tasks) > 0:
        logger.info(f"creating observations for {len(tasks)} {country.upper()} locations")
        await _execute_async_tasks(tasks)


def get_update_capacity_df(df: pd.DataFrame, rolling_capacity: bool = False) -> pd.DataFrame:
    """Get the rows that need to be updated based on capacity change.

    With ``rolling_capacity`` the capacity only ever goes up, never down.
    """

    # lets only consider non nans values
    df = df[~df["new_effective_capacity_watts"].isna()]

    if "update_capacity" in df.columns:
        # only update capacity if this is set to True
        # we use this in NL for non-validated capacities
        df = df[df['update_capacity']]

    # lets make sure we use the latest timestamp for each location_uuid and energy_source
    identity_cols = ["location_uuid"]
    if "energy_source" in df.columns:
        identity_cols.append("energy_source")
    if rolling_capacity:
        # rolling uses the first value above the old capacity instead, so earlier values still fit
        # effective_capacity_watts is the current capacity in the data platform
        df = df[df["solar_generation_kw"] * 1000 > df["effective_capacity_watts"]]
        df = df.sort_values(by="target_datetime_utc").groupby(identity_cols).head(1)
    else:
        df = df.sort_values(by="target_datetime_utc", ascending=False).groupby(identity_cols).head(1)

    current_cap = df["effective_capacity_watts"]
    new_cap = df["new_effective_capacity_watts"]

    # only update if the difference is more than one
    update_idx = (current_cap - new_cap).abs() >= 1

    deduped_df = df.loc[update_idx].sort_values(by="target_datetime_utc", ascending=False)

    # One join_key can hold several energy sources, e.g. RUVNL's solar and wind, so they must
    # be deduplicated separately or one source's update would be dropped.
    dedup_keys = [deduped_df.index]
    if "energy_source" in deduped_df.columns:
        dedup_keys.append(deduped_df["energy_source"])

    updates_df = deduped_df.groupby(dedup_keys).head(1).sort_index()
    return updates_df
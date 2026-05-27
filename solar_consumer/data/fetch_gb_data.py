import pandas as pd
import urllib.request
import urllib.parse
import json
import os
import yaml
from loguru import logger

from datetime import datetime, timedelta, timezone

from pvlive_api import PVLive

from solar_consumer.constants import GB_NESO_FORECAST_URL, GB_PVLIVE_DOMAIN_URL


def fetch_gb_data(historic_or_forecast: str = "forecast") -> pd.DataFrame:
    """
    Fetch data from the NESO API and process it into a Pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing two columns:
                      - `Datetime_GMT`: Combined date and time in UTC.
                      - `solar_forecast_kw`: Estimated solar forecast in kW.
    """

    if historic_or_forecast == "forecast":
        return fetch_gb_data_forecast()
    else:
        regime = os.getenv("UK_PVLIVE_REGIME", "in-day")
        return fetch_gb_data_historic(regime=regime)


def fetch_gb_data_forecast() -> pd.DataFrame:
    """
    Fetch data from the NESO API and process it into a Pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing two columns:
                      - `Datetime_GMT`: Combined date and time in UTC.
                      - `solar_forecast_kw`: Estimated solar forecast in kW.
    """
    meta_url = GB_NESO_FORECAST_URL
    response = urllib.request.urlopen(meta_url)
    data = json.loads(response.read().decode("utf-8"))

    # we take the latest path, which is the most recent forecast
    url = data["result"]["resources"][0]["path"]

    df = pd.read_csv(url)

    # Parse and combine DATE_GMT and TIME_GMT into Datetime_GMT
    df["Datetime_GMT"] = pd.to_datetime(
        df["DATE_GMT"].str[:10] + " " + df["TIME_GMT"].str.strip(),
        format="%Y-%m-%d %H:%M",
        errors="coerce",
    ).dt.tz_localize("UTC")

    # Rename and select necessary columns
    df["solar_forecast_kw"] = df["EMBEDDED_SOLAR_FORECAST"] * 1000
    df = df[["Datetime_GMT", "solar_forecast_kw"]]

    # Drop rows with invalid Datetime_GMT
    df = df.dropna(subset=["Datetime_GMT"])

    # rename columns to match the schema
    df.rename(
        columns={
            "solar_forecast_kw": "solar_generation_kw",
            "Datetime_GMT": "target_datetime_utc",
        },
        inplace=True,
    )

    return df


def load_gsp_merge_weights(config_path: str = None) -> dict:
    """
    Load GSP merge weight config from YAML.

    Returns a dict mapping each target GSP ID (int) to a list of source entries:
        {target_gsp_id: [{"gsp_id": int, "weight": float}, ...]}

    Missing or empty config files are handled gracefully — an empty dict is returned.
    """
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), "gsp_merge_weights.yaml")

    if not os.path.exists(config_path):
        logger.warning(f"No GSP merge weights config found at {config_path}")
        return {}

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if not raw:
        return {}

    result = {}
    for target_id, entry in raw.items():
        weights = entry.get("pvlive_merge_weights", [])
        result[int(target_id)] = [
            {"gsp_id": int(w["gsp_id"]), "weight": float(w["weight"])}
            for w in weights
        ]

    logger.info(f"Loaded GSP merge weights for {len(result)} target GSP IDs")
    return result


def fetch_gb_data_historic(regime: str) -> pd.DataFrame:
    """Fetch data from PVLive

    Args:
        - regime: regime of which to pull, either 'in-day' or 'day-after'.
        For 'in-day', we pull data from the last few hours to now + 30 minutes.
        For 'day-after', we pull data from 00:00 yesterday to 00:00 today.

    return a dataframe with the following columns:
        - target_datetime_utc: Datetime in UTC.
        - solar_generation_kw: Estimated solar generation in kW.
        - gsp_id: the gsp id, from 0 to 338
        - installedcapacity_mwp: installed capacity in MWp
        - capacity_mwp: capacity in MWp
        - regime: either 'in-day' or 'day-after'
        - pvlive_updated_utc: timestamp of when pvlive last updated the data
    """
    gb_pvlive_domain_url = os.getenv("GB_PVLIVE_DOMAIN_URL", GB_PVLIVE_DOMAIN_URL)
    pvlive = PVLive(domain_url=gb_pvlive_domain_url)

    # Load YAML-driven merge weights config. IDs present in this config will be
    # reconstructed from weighted combinations of replacement source GSP IDs
    # rather than being skipped outright.
    gsp_merge_weights = load_gsp_merge_weights()

    # Collect all source IDs that must be fetched to support remapping targets.
    required_source_ids: set[int] = set()
    for weights in gsp_merge_weights.values():
        for w in weights:
            required_source_ids.add(w["gsp_id"])

    datetime_utc = datetime.now(timezone.utc)

    if regime == "in-day":
        backfill_hours = int(os.getenv("UK_PVLIVE_BACKFILL_HOURS", 2))
        start = datetime_utc - timedelta(hours=backfill_hours)
        end = datetime_utc + timedelta(minutes=30)
    else:
        start = datetime_utc.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(hours=24)
        end = datetime_utc.replace(
            hour=0, minute=0, second=0, microsecond=0
        )   - timedelta(minutes=30)  # so we don't include 00:00

    all_gsps_yields = []

    # Use the live PVLive registry as the source of truth for valid GSP IDs.
    # This avoids a hardcoded ignore list — IDs that no longer exist in PVLive
    # simply won't appear here.
    gsp_ids = pvlive.gsp_ids
    n_gsps = int(os.getenv("UK_PVLIVE_MAX_GSP_ID", 342))
    if n_gsps is not None:
        gsp_ids = [id for id in gsp_ids if id < n_gsps]


    # Cache fetched DataFrames by gsp_id to avoid duplicate API calls when the
    # same source ID is shared across multiple remapping targets.
    fetched_cache: dict[int, pd.DataFrame] = {}

    # Append any merge-weights targets that aren't in the live registry
    # (retired/deprecated IDs that need reconstruction from their replacements).
    live_id_set = set(gsp_ids)
    gsp_ids_to_process = list(gsp_ids) + [
        gid for gid in gsp_merge_weights if gid not in live_id_set
    ]

    for gsp_id in gsp_ids_to_process:

        # If this ID is a remapping target, reconstruct from weighted sources.
        if gsp_id in gsp_merge_weights:
            weights_config = gsp_merge_weights[gsp_id]
            source_dfs = []

            for w in weights_config:
                source_id = w["gsp_id"]
                weight = w["weight"]

                # Fetch source if not already cached.
                if source_id not in fetched_cache:
                    logger.info(
                        f"Fetching source GSP ID {source_id} for remapping target {gsp_id}"
                    )
                    source_df = pvlive.between(
                        start=start,
                        end=end,
                        entity_type="gsp",
                        entity_id=source_id,
                        dataframe=True,
                        extra_fields="installedcapacity_mwp,capacity_mwp,updated_gmt",
                    )
                    fetched_cache[source_id] = source_df

                weighted = fetched_cache[source_id].copy()
                weighted["generation_mw"] = weighted["generation_mw"] * weight
                source_dfs.append(weighted)

            if not source_dfs:
                logger.warning(
                    f"No source data found for remapped GSP ID {gsp_id}, skipping"
                )
                continue

            # Sum weighted generation across all sources, aligned by timestamp.
            base = source_dfs[0][["datetime_gmt"]].copy()
            base["generation_mw"] = sum(
                df.set_index("datetime_gmt")["generation_mw"]
                for df in source_dfs
            ).values

            # Use capacity/metadata from first source as best approximation.
            base["installedcapacity_mwp"] = source_dfs[0]["installedcapacity_mwp"].values
            base["capacity_mwp"] = source_dfs[0]["capacity_mwp"].values
            base["updated_gmt"] = source_dfs[0]["updated_gmt"].values
            gsp_yield_df = base

            logger.info(
                f"Reconstructed GSP ID {gsp_id} from {len(weights_config)} source(s)"
            )

        # Normal direct fetch.
        else:
            logger.info(
                f"Getting data for GSP ID {gsp_id}, out of {len(gsp_ids_to_process)} GSPs, for regime {regime}"
            )
            gsp_yield_df = pvlive.between(
                start=start,
                end=end,
                entity_type="gsp",
                entity_id=gsp_id,
                dataframe=True,
                extra_fields="installedcapacity_mwp,capacity_mwp,updated_gmt",
            )
            # Cache in case this ID is needed as a source for a later remapping target.
            fetched_cache[gsp_id] = gsp_yield_df

        logger.debug(
            f"Got {len(gsp_yield_df)} gsp yield for gsp id {gsp_id} before filtering"
        )

        # TODO if did not find any values,
        # https://github.com/openclimatefix/solar-consumer/issues/104
        # Make nighttime zeros

        # capacity is zero, set generation to 0
        if gsp_yield_df["capacity_mwp"].sum() == 0:
            gsp_yield_df["generation_mw"] = 0

        # drop nan value in generation_mw column if not all are nans
        # this gets rid of last value if it is nan
        if not gsp_yield_df["generation_mw"].isnull().all():
            gsp_yield_df = gsp_yield_df.dropna(subset=["generation_mw"])

        # need columns datetime_utc, solar_generation_kw
        gsp_yield_df["solar_generation_kw"] = 1000 * gsp_yield_df["generation_mw"]
        gsp_yield_df["target_datetime_utc"] = gsp_yield_df["datetime_gmt"]
        gsp_yield_df["pvlive_updated_utc"] = pd.to_datetime(gsp_yield_df["updated_gmt"])

        # Convert capacity to kW
        gsp_yield_df["capacity_kw"] = gsp_yield_df["capacity_mwp"] * 1000
        gsp_yield_df["capacity_no_degradation_kw"] = gsp_yield_df["installedcapacity_mwp"] * 1000

        gsp_yield_df = gsp_yield_df[
            [
                "solar_generation_kw",
                "target_datetime_utc",
                "capacity_kw",
                "capacity_no_degradation_kw",
                "pvlive_updated_utc",
            ]
        ]
        gsp_yield_df["regime"] = regime
        gsp_yield_df["gsp_id"] = gsp_id

        all_gsps_yields.append(gsp_yield_df)

        # TODO back up
        # if there is national but no gsps, make gsp from national
        # https://github.com/openclimatefix/solar-consumer/issues/105

    return pd.concat(all_gsps_yields, ignore_index=True)

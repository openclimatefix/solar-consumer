import pandas as pd
import urllib.request
import urllib.parse
import json
import os
from loguru import logger

from datetime import datetime, timedelta, timezone

from pvlive_api import PVLive

from nowcasting_datamodel.connection import DatabaseConnection
from nowcasting_datamodel.models.gsp import LocationSQL


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
    meta_url = "https://api.neso.energy/api/3/action/datapackage_show?id=embedded-wind-and-solar-forecasts"
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

    pvlive_domain_url = "api.pvlive.uk"
    pvlive = PVLive(domain_url=pvlive_domain_url)
    # ignore these gsp ids from PVLive as they are no longer used
    ignore_gsp_ids = [5, 17, 53, 75, 139, 140, 143, 157, 163, 225, 310]

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
    national_df = None
    missing_gsps = []
    n_gsps = int(os.getenv("UK_PVLIVE_N_GSPS", 342))
    for gsp_id in range(0, n_gsps + 1):
        if gsp_id in ignore_gsp_ids:
            continue

        logger.info(
            f"Getting data for GSP ID {gsp_id}, out of {n_gsps} GSPs, for regime {regime}"
        )

        gsp_yield_df: pd.DataFrame = pvlive.between(
            start=start,
            end=end,
            entity_type="gsp",
            entity_id=gsp_id,
            dataframe=True,
            extra_fields="installedcapacity_mwp,capacity_mwp,updated_gmt",
        )

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
        gsp_yield_df = gsp_yield_df[
            [
                "solar_generation_kw",
                "target_datetime_utc",
                "installedcapacity_mwp",
                "capacity_mwp",
                "pvlive_updated_utc",
            ]
        ]
        gsp_yield_df["regime"] = regime
        gsp_yield_df["gsp_id"] = gsp_id

        if gsp_yield_df.empty:
            missing_gsps.append(gsp_id)
        else:
            all_gsps_yields.append(gsp_yield_df)
            if gsp_id == 0:
                national_df = gsp_yield_df.copy()

        # TODO back up
        # if there is national but no gsps, make gsp from national
        # https://github.com/openclimatefix/solar-consumer/issues/105

    if missing_gsps and national_df is not None:
        logger.info(f"Creating backup data for {len(missing_gsps)} missing GSPs using national data")
        db_url = os.getenv("DB_URL")
        use_db_backup = os.getenv("ENABLE_DB_BACKUP_GSP", "false").lower() in ("1", "true", "yes")
        if not db_url or not use_db_backup:
            if not db_url:
                logger.warning("DB_URL not set, cannot create backup GSP data")
            else:
                logger.info("ENABLE_DB_BACKUP_GSP is not enabled; skipping DB-backed backup GSP data")
        else:
            try:
                connection = DatabaseConnection(url=db_url)
                with connection.get_session() as session:
                    locations = (
                        session.query(LocationSQL)
                        .filter(LocationSQL.gsp_id.in_(missing_gsps))
                        .all()
                    )
                    backup_rows = []
                    for _, national_row in national_df.iterrows():
                        national_capacity = national_row['installedcapacity_mwp']
                        if national_capacity == 0 or pd.isna(national_capacity):
                            continue
                        for location in locations:
                            if (
                                location.installed_capacity_mw is not None
                                and location.installed_capacity_mw > 0
                            ):
                                factor = location.installed_capacity_mw / national_capacity
                                new_row = national_row.copy()
                                new_row['solar_generation_kw'] *= factor
                                new_row['gsp_id'] = location.gsp_id
                                new_row['installedcapacity_mwp'] = location.installed_capacity_mw
                                new_row['capacity_mwp'] = location.installed_capacity_mw
                                backup_rows.append(new_row)
                    if backup_rows:
                        backup_df = pd.DataFrame(backup_rows)
                        all_gsps_yields.append(backup_df)
                        logger.info(f"Created backup data for {len(backup_rows)} entries")
            except Exception as e:
                logger.error(f"Error creating backup GSP data: {e}")

    if not all_gsps_yields:
        return pd.DataFrame(
            columns=[
                "target_datetime_utc",
                "solar_generation_kw",
                "gsp_id",
                "installedcapacity_mwp",
                "capacity_mwp",
                "regime",
                "pvlive_updated_utc",
            ]
        )

    return pd.concat(all_gsps_yields, ignore_index=True)

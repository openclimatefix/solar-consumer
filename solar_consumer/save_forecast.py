from loguru import logger
from nowcasting_datamodel.save.save import save
from pvsite_datamodel.write.generation import insert_generation_values
from pvsite_datamodel.write.forecast import insert_forecast_values
from pvsite_datamodel.read.site import get_site_by_client_site_name
from pvsite_datamodel.write.user_and_site import create_site
from pvsite_datamodel.pydantic_models import PVSiteEditMetadata as PVSite
from sqlalchemy.orm.session import Session
import os
import pandas as pd


nl_national = PVSite(client_site_name="nl_national", latitude="52.15", longitude="5.23")


def save_generation_to_site_db(
    generation_data: pd.DataFrame, session: Session, country: str = "nl"
):
    """Save generation data to the database.

    Parameters:
        generation_data (pd.DataFrame): DataFrame containing generation data to save.
            The following columns must be present: solar_generation_kw, target_datetime_utc and capacity_kw
        session (Session): SQLAlchemy session for database access.
        country: (str): Country code for the generation data. Currently only 'nl' is supported.

    """
    # Check if generation_data is empty
    if generation_data.empty:
        logger.warning("No generation data provided to save!")
        return

    if country != "nl":
        raise Exception("Only NL generation data is supported when saving (atm).")

    try:
        logger.info("Saving generation data to the database.")

        # get site uuid
        site = get_or_create_site(session)

        # add site_uuid to the generation data
        generation_data["site_uuid"] = site.site_uuid

        # rename columns to match the database schema
        generation_data.rename(
            columns={
                "solar_generation_kw": "power_kw",
                "target_datetime_utc": "start_utc",
            },
            inplace=True,
        )

        # make sure start_utc is datetime
        generation_data["start_utc"] = pd.to_datetime(generation_data["start_utc"])

        insert_generation_values(
            session=session,
            df=generation_data,
        )
        session.commit()
        logger.info(f"Successfully saved {len(generation_data)} rows of generation data.")

        # update capacity
        if "capacity_kw" in generation_data.columns:
            capacity_kw = int(generation_data["capacity_kw"].max())
            if capacity_kw > site.capacity_kw + 1.0:
                old_site_capacity_kw = site.capacity_kw
                site.capacity_kw = capacity_kw
                session.commit()
                logger.info(
                    f"Updated site {nl_national.client_site_name} capacity "
                    f"from {old_site_capacity_kw} to {site.capacity_kw} kW."
                )

    except Exception as e:
        logger.error(f"An error occurred while saving generation data: {e}")
        raise e


def get_or_create_site(session):
    """Get or create a site in the database."""
    try:
        site = get_site_by_client_site_name(
            session=session,
            client_site_name=nl_national.client_site_name,
            client_name=nl_national.client_site_name,  # this is not used
        )
    except Exception:
        logger.info(f"Creating site {nl_national.client_site_name} in the database.")
        site, _ = create_site(
            session=session,
            latitude=nl_national.latitude,
            longitude=nl_national.longitude,
            client_site_name=nl_national.client_site_name,
            client_site_id=1,
            country="nl",
            capacity_kw=20_000_000,
            dno="",  # these are UK specific things
            gsp="",  # these are UK specific things
        )
    return site


def save_forecasts_to_site_db(
    forecast_data: pd.DataFrame,
    session: Session,
    model_tag: str,
    model_version: str,
    country: str = "nl",
):
    """Save generation data to the database.

    Parameters:
        forecast_data (pd.DataFrame): DataFrame containing generation data to save.
            The following columns must be present: solar_generation_kw and target_datetime_utc
        session (Session): SQLAlchemy session for database access.
        model_tag (str): Model tag to fetch model metadata.
        model_version (str): Model version to fetch model metadata.
        country: (str): Country code for the generation data. Currently only 'nl' is supported.

    """

    if country != "nl":
        raise Exception("Only NL generation data is supported when saving (atm).")

    site = get_or_create_site(session)

    timestamp_utc = pd.Timestamp.now(tz="UTC").floor("15min")

    forecast_meta = {
        "location_uuid": site.location_uuid,
        "timestamp_utc": timestamp_utc,
        "forecast_version": model_version,
    }

    forecast_data.rename(
        columns={
            "solar_generation_kw": "forecast_power_kw",
            "target_datetime_utc": "start_utc",
        },
        inplace=True,
    )

    # drop other rows and add end_utc
    forecast_data = forecast_data[["forecast_power_kw", "start_utc"]]
    forecast_data["end_utc"] = forecast_data["start_utc"] + pd.Timedelta(hours=0.25)

    # calculate horizon minutes
    forecast_data["horizon_minutes"] = (
        forecast_data["start_utc"] - timestamp_utc
    ).dt.total_seconds() / 60

    insert_forecast_values(
        forecast_values_df=forecast_data,
        forecast_meta=forecast_meta,
        ml_model_name=model_tag,
        ml_model_version=model_version,
        session=session,
    )


def save_forecasts_to_db(forecasts: list, session: Session):
    """Save forecasts to the database.

    Parameters:
        forecasts (list): List of forecast objects to save.
        session (Session): SQLAlchemy session for database access.

    Return:
        None
    """
    # Check if forecasts is empty
    if not forecasts:
        logger.warning("No forecasts provided to save!")
        return

    try:
        logger.info("Saving forecasts to the database.")
        save(
            forecasts=forecasts,
            session=session,
        )
        logger.info(f"Successfully saved {len(forecasts)} forecasts to the database.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts: {e}")
        raise e


def save_forecasts_to_csv(forecasts: pd.DataFrame, csv_dir: str):
    """Save forecasts to a CSV file.

    Parameters:
        forecasts (pd.DataFrame): DataFrame containing forecast data to save.
        csv_dir (str): Directory to save CSV files.

    Return:
        None
    """
    # Check if forecasts is empty
    if forecasts.empty:
        logger.warning("No forecasts provided to save!")
        return

    if not csv_dir:  # check if directory csv directory provided
        raise ValueError("CSV directory is not provided for CSV saving.")

    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, "forecast_data.csv")

    try:
        forecasts.drop(
            columns=["_sa_instance_state"], errors="ignore", inplace=True
        )  # Remove SQLAlchemy metadata

        logger.info(f"Saving forecasts to CSV at {csv_path}")
        forecasts.to_csv(csv_path, index=False)
        logger.info(f"Successfully saved {len(forecasts)} forecasts to CSV.")
    except Exception as e:
        logger.error(f"An error occurred while saving forecasts to CSV: {e}")
        raise e

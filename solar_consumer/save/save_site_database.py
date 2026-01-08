from loguru import logger
from pvsite_datamodel.write.generation import insert_generation_values
from pvsite_datamodel.write.forecast import insert_forecast_values
from pvsite_datamodel.read.site import get_site_by_client_site_name
from pvsite_datamodel.write.user_and_site import create_site
from pvsite_datamodel.pydantic_models import PVSiteEditMetadata as PVSite
from sqlalchemy.orm.session import Session
import pandas as pd
from typing import Optional

# Default NL national site, and NL regional
nl_national = PVSite(client_site_name="nl_national", latitude="52.13", longitude="5.29")
nl_region_1 = PVSite(client_site_name="nl_region_1_groningen", latitude="53.22", longitude="6.74")
nl_region_2 = PVSite(client_site_name="nl_region_2_friesland", latitude="53.11", longitude="5.85")
nl_region_3 = PVSite(client_site_name="nl_region_3_drenthe", latitude="52.86", longitude="6.62")
nl_region_4 = PVSite(client_site_name="nl_region_4_overijssel", latitude="52.45", longitude="6.45")
nl_region_5 = PVSite(client_site_name="nl_region_5_flevoland", latitude="52.53", longitude="5.60")
nl_region_6 = PVSite(client_site_name="nl_region_6_gelderland", latitude="52.06", longitude="5.95")
nl_region_7 = PVSite(client_site_name="nl_region_7_utrecht", latitude="52.08", longitude="5.17")
nl_region_8 = PVSite(client_site_name="nl_region_8_noord_holland", latitude="52.58", longitude="4.87")
nl_region_9 = PVSite(client_site_name="nl_region_9_zuid_holland", latitude="51.94", longitude="4.47")
nl_region_10 = PVSite(client_site_name="nl_region_10_zeeland", latitude="51.45", longitude="3.84")
nl_region_11 = PVSite(client_site_name="nl_region_11_noord_brabant", latitude="51.56", longitude="5.20")
nl_region_12 = PVSite(client_site_name="nl_region_12_limburg", latitude="51.21", longitude="5.94")
NL_NATIONAL_AND_REGIONS = {"0": nl_national,
                            "1": nl_region_1, "2": nl_region_2, "3": nl_region_3,
                            "4": nl_region_4, "5": nl_region_5, "6": nl_region_6,
                            "7": nl_region_7, "8": nl_region_8, "9": nl_region_9, 
                            "10": nl_region_10, "11": nl_region_11, "12": nl_region_12
                         }

# Germany Transmission System Operators (TSOs)
# Coords ~direct to HQs
de_50hertz = PVSite(client_site_name="50Hertz", latitude="52.53", longitude="13.37")
de_amprion = PVSite(client_site_name="Amprion", latitude="51.52", longitude="7.45")
de_tennet = PVSite(client_site_name="TenneT", latitude="52.38", longitude="5.17")
de_transnetbw = PVSite(client_site_name="TransnetBW", latitude="48.78", longitude="9.18")
DE_TSO_SITES = {"TransnetBW": de_transnetbw, "50Hertz": de_50hertz, "TenneT": de_tennet,
                "Amprion": de_amprion}
# Actual installed capacities (in kW) by TSO
# 50Hz via 2022 report, all others via 2020 OSPD dataset
DE_TSO_CAPACITY = {"TransnetBW": 10_770_000, "50Hertz": 18_175_000, "TenneT": 21_882_000, 
                   "Amprion": 16_506_000}


def validate_nl_capacities(generation_data: pd.DataFrame, tolerance: float = 0.001) -> bool:
    """
    Validate that the sum of NL regional capacities matches the national capacity.
    
    This prevents "funky" capacity data from being saved to the database by checking
    if the sum of regional capacities is within tolerance of the national capacity.
    
    Parameters:
        generation_data (pd.DataFrame): DataFrame with columns 'region_id' and 'capacity_kw'
        tolerance (float): Acceptable deviation ratio (default 0.001 = 0.1%)
                          This allows ~30MW difference for NL (~30,000 kW capacity)
    
    Returns:
        bool: True if capacities are valid, False if they're "funky"
    """
    
    # Group by timestamp to check each time period separately
    for timestamp, group in generation_data.groupby('target_datetime_utc'):
        
        # Get national capacity (region_id = 0)
        national_rows = group[group['region_id'] == 0]
        if national_rows.empty or pd.isna(national_rows['capacity_kw'].iloc[0]):
            logger.warning(f"Missing national capacity data for {timestamp}, skipping validation")
            continue
            
        national_capacity = national_rows['capacity_kw'].iloc[0]
        
        # Get sum of regional capacities (region_id 1-12)
        regional_rows = group[group['region_id'] != 0]
        if regional_rows.empty:
            logger.warning(f"No regional data for {timestamp}, skipping validation")
            continue
            
        regional_capacity_sum = regional_rows['capacity_kw'].sum()
        
        # Calculate ratio
        if national_capacity == 0:
            logger.error(f"National capacity is zero for {timestamp}, failing validation")
            return False
            
        ratio = regional_capacity_sum / national_capacity
        
        # Check if ratio is outside tolerance bounds
        if ratio >= (1 + tolerance) or ratio <= (1 - tolerance):
            logger.warning(
                f"Funky capacity data detected for {timestamp}:\n"
                f"  National capacity: {national_capacity:,.0f} kW\n"
                f"  Regional sum: {regional_capacity_sum:,.0f} kW\n"
                f"  Ratio: {ratio:.6f}\n"
                f"  Tolerance bounds: [{1-tolerance:.6f}, {1+tolerance:.6f}]\n"
                f"  Skipping this batch to prevent bad data from entering database."
            )
            return False
    
    # All timestamps passed validation
    logger.info("NL capacity validation passed - data looks good!")
    return True

def get_or_create_pvsite(
    session: Session, pvsite: PVSite, country: str, capacity_override_kw: Optional[int] = None,
):
    """
    Retrieve PVsite record by name or create if missing

    If `capacity_override_kw` is provided, that value will be used when creating the site. 
    For Germany, the TSO’s known capacity is used if no override is given. For NL, default 20GW applied

    Parameters:
        session (Session): CurrentSQLAlchemy session
        pvsite (PVSite): Pydantic model with site metadata
        country (str): Country code ('nl' or 'de')
        capacity_override_kw (Optional[int]): Force a specific capacity on creation

    Returns:
        site: The existing/created site model instance
    """                    

    try:
        site = get_site_by_client_site_name(
            session=session,
            client_site_name=pvsite.client_site_name,
            client_name=pvsite.client_site_name, # this is not used
        )
    except Exception:
        logger.info(f"Creating site {pvsite.client_site_name} in the database.")
        
        # Choose capacity based on country; per-TSO for de; nl only has 20GW hard‑coded
        if capacity_override_kw is not None:
            capacity = capacity_override_kw
        elif country == "de":
            capacity = DE_TSO_CAPACITY[pvsite.client_site_name]
        else:
            capacity = 20_000_000
        
        site, _ = create_site(
            session=session,
            latitude=pvsite.latitude,
            longitude=pvsite.longitude,
            client_site_name=pvsite.client_site_name,
            client_site_id=1,
            country=country,
            capacity_kw=capacity,
            dno="", # these are UK specific things
            gsp="", # these are UK specific things
        )
    return site

def update_capacity(
    session: Session, site, capacity_override_kw: Optional[int],
):
    """
    Update stored site capacity if the override is higher. Only runs when importing generation
    data so DB always reflects highest observed capacity.

    Parameters:
        session (Session): Active session
        site: The site database model instance
        capacity_override_kw (Optional[int]): New capacity candidate to compare

    Returns:
        None
    """
  
    if capacity_override_kw is not None and (abs(capacity_override_kw - site.capacity_kw) >= 1.0):
        old_site_capacity_kw = site.capacity_kw
        site.capacity_kw = capacity_override_kw
        session.commit()
        logger.info(
            f"Updated site {site.client_location_name} capacity from {old_site_capacity_kw } to {site.capacity_kw} kW."
        )
  

def save_generation_to_site_db(
    generation_data: pd.DataFrame, session: Session, country: str = "nl"
):
    """Save generation data to the database.

    Parameters:
        generation_data (pd.DataFrame): DataFrame containing generation data to save.
            The following columns must be present: 
            - solar_generation_kw
            - target_datetime_utc
            - capacity_kw (only when country="nl")
            - tso_zone (only when country="de")
        session (Session): SQLAlchemy session for database access.
        country: (str): Country code for the generation data ('nl' or 'de')
    
    Return:
        None
    """

    # Check if generation_data is empty
    if generation_data.empty:
        logger.warning("No generation data provided to save!")
        return

    if country == "nl":
        # Check if we have capacity data
        if 'capacity_kw' in generation_data.columns and 'region_id' in generation_data.columns:
            if not validate_nl_capacities(generation_data, tolerance=0.001):
                logger.error("NL capacity validation failed - aborting save to prevent funky data!")
                return
        else:
            logger.warning("Missing capacity_kw or region_id columns - skipping validation")
    
    # Determine country
    if country == "nl":
        country_sites = NL_NATIONAL_AND_REGIONS
    elif country == "de":
        country_sites = DE_TSO_SITES
    else:
        raise Exception("Only generation data from the following countries is supported \
            when saving: 'nl', 'de'")

    # Loop per site
    for key, pvsite in country_sites.items():
        
        # Filter by TSO for Germany, or use all data for NL
        if country == "de":
            generation_data_tso_df = generation_data[generation_data["tso_zone"] == key].copy()
        elif country == "nl":
            generation_data_tso_df = generation_data[generation_data["region_id"] == int(key)].copy()
        else:
            generation_data_tso_df = generation_data.copy()
            
        if generation_data_tso_df.empty:
            logger.debug(f"No rows for {key!r}, skipping")
            continue

        # Derive capacity override once
        capacity_override = None
        if "capacity_kw" in generation_data_tso_df.columns:
            max_capacity = generation_data_tso_df["capacity_kw"].max()
            if not pd.isna(max_capacity):
                capacity_override = int(max_capacity)
        
        # Create or fetch site and pass same override for any country
        site = get_or_create_pvsite(session, pvsite, country, 
                                    capacity_override_kw=capacity_override,)

        # Prepare DataFrame, rename and insert
        generation_data_tso_df = generation_data_tso_df.rename(
            columns={
                "solar_generation_kw": "power_kw",
                "target_datetime_utc": "start_utc",
            }
        )
        generation_data_tso_df["start_utc"] = pd.to_datetime(generation_data_tso_df["start_utc"])
        generation_data_tso_df["site_uuid"] = site.location_uuid

        insert_generation_values(session=session, df=generation_data_tso_df)
        session.commit()
        update_capacity(session, site, capacity_override_kw=capacity_override,)
        logger.info(f"Successfully saved {len(generation_data_tso_df)} rows")


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
        
    Return:
        None
    """

    if country != "nl":
        raise Exception("Only NL forecast data is supported when saving (atm).")

    site = get_or_create_pvsite(session, nl_national, country)

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

'''
The ENTSO-E API is used to collect the day-ahead electricity prices for France. The ENTSO-E API requires an API key, which is 
obtained by registering for a free account on the ENTSO-E Transparency Platform. 

The French regional solar PV data is collected via two sources. First the 'real-time' (temps-real, tr) data, which only goes back 
4 months from the time of downloading. The second source is the 'consolidated and final' (consolide-definees, cons-def) data, 
which starts at the cutoff of the temps-real data and goes back to 2012. 

This script downloads the two solar generation datasets to merge into one master dataset between two user specified dates. The ENTSO-e
price data is also downloaded for the same dates. 

'''

import logging
import os

import pandas as pd
import requests
from entsoe import ENTSOEClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------ USER CONFIGURABLE PARAMETERS ---------------------

# ENTSO-E API Key (preferably set via environment variable ENTSOE_API_KEY)
ENTSOE_API_KEY = os.environ["ENTSOE_API_KEY"]

# File export directory
OUTPUT_DIR = "./download_data_try"

# Date ranges for data retrieval
START_DATE = "2020-01-01"
END_DATE = None  # Defaults to current date if None
TR_WINDOW_DAYS = 130  # Real-time lookback window (max ~4 months)

# RTE Open Data URLs
REAL_TIME_URL = ("https://odre.opendatasoft.com/api/v2/catalog/datasets/eco2mix-regional-tr/exports/json")
CONS_URL = ("https://odre.opendatasoft.com/api/v2/catalog/datasets/eco2mix-regional-cons-def/exports/json")

# ----------------------------------------------------------------------------


def get_entsoe_prices(api_key, start_time, end_time = None):
    '''Retrieving day-ahead prices for France from ENTSO-E.

    Args: 
        API_KEY (str): Generated from ENTSO-E account.
        start_time (str): Earliest date for data retrieval, YYYY-MM-DD.
        end_time (str): End date for data retrieval, defaults to present date, YYYY-MM-DD.

    Returns: 
        DataFrame with two re-named columns 'timestamp' and 'price_eur_mwh'.
    '''

    if end_time is None:
        start_time = pd.Timestamp(start_time, tz='Europe/Paris')
        end_time = pd.Timestamp.now(tz = 'Europe/Paris')
    else:
        start_time = pd.Timestamp(start_time, tz='Europe/Paris')
        end_time = pd.Timestamp(end_time, tz='Europe/Paris')

    client = ENTSOEClient(api_key=api_key)
    price_series = client.prices.day_ahead(
        country='FR', 
        start=start_time, 
        end=end_time)
    
    df_price_series = pd.DataFrame(price_series).reset_index()
    df_prices_timestamp = df_price_series[['timestamp', 'value']].copy()
    df_prices_timestamp.columns = ['timestamp', 'price_eur_mwh']

    return df_prices_timestamp



def get_realtime_rte_solar_generation(tr_url, chosen_date = None, window_days = 130):
    '''
    Retrieving solar generation data for France from RTE's open data portal for the real-time (tr) dataset (4 months).

    Args: 
        tr_url (str): URL for the real-time dataset.
        file_path (str): File path to directory where the master CSV file will be saved.
        chosen_date (str, optional): A specified date within the last 4 months (str, format 'YYYY-MM-DD') or None to use the last 
            130 days from today. 
        window_days (int, default 130): Window of days to retrieve data. There is a hard limit on the earliest date for this dataset, 
                4 months back from the current date.

    Returns: 
        DataFrame containing regional solar generation data. Columns include 'region', 'timestamp', 
        'solar_generation_mw', and 'capacity_percent'.
    '''
    
    # Filtering condition if date is given or none:
    if chosen_date is None:
        anchor_dt = pd.Timestamp.now()
    else:
        anchor_dt = pd.to_datetime(chosen_date)

    start_dt = anchor_dt - pd.Timedelta(days=window_days)  
    end_dt = anchor_dt + pd.Timedelta(days=1)  # +1 day so includes full anchor date

    start_str = start_dt.strftime('%Y-%m-%d')
    end_str = end_dt.strftime('%Y-%m-%d')
    
    where_clause = f"date_heure >= '{start_str}' AND date_heure < '{end_str}'"

    params = {
        "where": where_clause,
        "select": "libelle_region, date_heure, solaire, tch_solaire",
        "limit": -1}

    logger.info(f"Downloading real-time data range: {start_str} to {end_str}...")
    response = requests.get(tr_url, params=params)
    
    if response.status_code != 200 or not response.json():
        raise ValueError(f'No real time data found, HTTP {response.status_code}.')

    # Re-naming Frence columns names:
    solar_PV_df_tr = (pd.DataFrame(response.json())
        .rename(columns={
            'libelle_region': 'region',
            'date_heure': 'timestamp',
            'solaire': 'solar_generation_mw',
            'tch_solaire': 'capacity_percent'
        })
        .assign(timestamp=lambda x: pd.to_datetime(x['timestamp']))
        .sort_values(by=['region', 'timestamp'])
        .reset_index(drop=True))

    return solar_PV_df_tr



def get_cons_def_rte_solar_generation(cons_def_url, start_date="2020-01-01", end_date=None):
    '''
    Retrieving  consolidated/verified solar generation data for France from RTE's ODRÉ portal, which starts 4 months before current 
    date and goes back to 2012.
    
    Args:
        cons_def_url (str): URL for the consolidated/verified dataset.
        file_path (str): File path to directory where the output CSV will be saved.
        start_date (str): Start date string (format 'YYYY-MM-DD'). Defaults to '2020-01-01'.
        end_date (str, optional): End date string (format 'YYYY-MM-DD'). If None, defaults to the current date.

    Returns:
        DataFrame containing regional solar generation data. Columns include 'region', 'timestamp', 
        'solar_generation_mw', and 'capacity_percent'.
    '''
    
    start_dt = pd.to_datetime(start_date)
    
    if end_date is None:
        end_dt = pd.Timestamp.now()
    else:
        end_dt = pd.to_datetime(end_date)

    # Advance end_dt by +1 day so '< end_str' captures the full final day timestamps up to 23:30
    fetch_end_dt = end_dt + pd.Timedelta(days=1)

    start_str = start_dt.strftime('%Y-%m-%d')
    end_str = fetch_end_dt.strftime('%Y-%m-%d')
    label_end_str = end_dt.strftime('%Y-%m-%d')

    where_clause = f"date_heure >= '{start_str}' AND date_heure < '{end_str}'"

    params = {"where": where_clause,
        "select": "libelle_region, date_heure, solaire, tch_solaire",
        "limit": -1}

    logger.info(f"Downloading consolidated data range: {start_str} to {label_end_str}...")
    response = requests.get(cons_def_url, params=params)

    if response.status_code != 200:
        raise ValueError(f'No consolidated/validated data found, HTTP {response.status_code}.')

    records = response.json()
    if not records:
        raise ValueError(f"No records found for range {start_str} to {label_end_str}.")
        
    # Renaming the French column names:
    df_cons = (pd.DataFrame(records)
        .rename(columns={
            'libelle_region': 'region',
            'date_heure': 'timestamp',
            'solaire': 'solar_generation_mw',
            'tch_solaire': 'capacity_percent'
        })
        .assign(timestamp=lambda x: pd.to_datetime(x['timestamp']))
        .sort_values(by=["region", "timestamp"])
        .reset_index(drop=True))

    return df_cons


def get_rte_solar_data_full(file_path, start_date="2020-01-01", end_date=None, tr_window_days=130, 
                            cons_url = CONS_URL, tr_url = REAL_TIME_URL):
    '''
    Wrapper to retrieve full historical and real-time solar generation data for France by combining 
    the consolidated (cons-def) and real-time (tr) RTE datasets.
    
    Args:
        file_path (str): File path to directory where the output CSV files will be saved.
        start_date (str): Historical start date (format 'YYYY-MM-DD'). Defaults to '2020-01-01'.
        end_date (str, optional): Target end date. Defaults to current date if None.
        tr_window_days (int): Lookback window in days for the real-time dataset (default 130).

    Returns:
        DataFrame: Merged, deduplicated master DataFrame sorted by region and timestamp.
    '''
    
    anchor_dt = pd.Timestamp.now() if end_date is None else pd.to_datetime(end_date)
    anchor_str = anchor_dt.strftime('%Y-%m-%d')

    df_cons = get_cons_def_rte_solar_generation(cons_def_url= cons_url, start_date=start_date, end_date=anchor_dt)
    
    df_tr = get_realtime_rte_solar_generation(tr_url= tr_url, chosen_date=anchor_str, window_days=tr_window_days)

    df_list = [df for df in [df_cons, df_tr] if not df.empty]
    
    if not df_list:
        raise ValueError("No data retrieved from real time or consolidated/validated data sources.")
        
    df_full = (pd.concat(df_list, ignore_index=True)
        # Keep real-time ('tr') row if there happens to be an overlapping timestamp
        .drop_duplicates(subset=['region', 'timestamp'], keep='last')
        .sort_values(by=['region', 'timestamp'])
        .reset_index(drop=True))

    return df_full


def main():
    """Main execution workflow to extract solar PV generation and price data."""

    # Extract full regional solar PV dataset (consolidated + real-time)
    df_solar = get_rte_solar_data_full(
        file_path=OUTPUT_DIR,
        start_date=START_DATE,
        end_date=END_DATE,
        tr_window_days=TR_WINDOW_DAYS,
        cons_url=CONS_URL,
        tr_url=REAL_TIME_URL,)

    # Saving the merged regional solar generation dataset:
    start_label = pd.to_datetime(START_DATE).strftime('%Y-%m-%d')
    end_dt = (
        pd.Timestamp.now() if END_DATE is None else pd.to_datetime(END_DATE)
    )
    end_label = end_dt.strftime('%Y-%m-%d')

    out_file_gen_data = os.path.join(OUTPUT_DIR, f"france_regional_solar_master_{start_label}_to_{end_label}.csv")
    df_solar.to_csv(out_file_gen_data, index=False)

    # Extracting the day ahead price data for France:
    df_prices = get_entsoe_prices(api_key=ENTSOE_API_KEY, start_time=START_DATE, end_time=END_DATE)

    # Saving the ENTSO-E price data:
    out_file_price_data = os.path.join(OUTPUT_DIR, f"france_day_ahead_prices_{start_label}_to_{end_label}.csv")
    df_prices.to_csv(out_file_price_data, index = False)


if __name__ == "__main__":
    main()

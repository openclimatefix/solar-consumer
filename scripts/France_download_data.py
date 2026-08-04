'''
The ENTSO-E API is used to collect the day-ahead electricity prices for France. The ENTSO-E API requires an API key, which is 
obtained by registering for a free account on the ENTSO-E Transparency Platform. 

The French regional solar PV data is collected via two sources. First the 'real-time' (temps-real, tr) data, which only goes back 
4 months from the time of downloading. The second source is the 'consolidated and final' (consolide-definees, cons-def) data, which starts
at the cutoff of the temps-real data and goes back to 2012. Two functions are provided to retrieve each dataset separately, and a 
third function is provided to combine the two datasets into a single master DataFrame.
'''

from entsoe import ENTSOEClient
import pandas as pd
import requests
import os

def get_entsoe_prices(API_KEY, start_time, end_time):
    '''
    Retrieving day-ahead prices for France from ENTSO-E.
    Inputs: API_KEY (str) generated from ENTSO-E account, start_time (pd.Timestamp), end_time (pd.Timestamp). Both timestamps should be
    in the 'Europe/Paris' timezone.  

    Output: DataFrame with two re-named columns 'Timestamp' and 'Price_EUR_MWh'.
    '''
    client = ENTSOEClient(api_key=API_KEY)
    price_series = client.prices.day_ahead(
        country='FR', 
        start=start_time, 
        end=end_time)
    
    df_price_series = pd.DataFrame(price_series).reset_index()
    df_prices_timestamp = df_price_series[['timestamp', 'value']].copy()
    df_prices_timestamp.columns = ['Timestamp', 'Price_EUR_MWh']

    return df_prices_timestamp


# First source of data: real-time solar generation data (tr) from RTE's ODRÉ portal
real_time_url = "https://odre.opendatasoft.com/api/v2/catalog/datasets/eco2mix-regional-tr/exports/json"

def get_rte_solar_data_tr(tr_url, file_path, chosen_date = None, window_days = 130):
    '''
    Retrieving solar generation data for France from RTE's open data portal for the real-time (tr) dataset (4 months).

    Inputs: 
        tr_url (str): URL for the real-time dataset
        file_path (str): where the master CSV file will be saved
        chosen_date (str, optional): a specified date within the last 4 months (str, format 'YYYY-MM-DD') or None to use the last 130 days
                 from today. 
        window_days (int, default 130): Window days to retrieve data. There is a hard limit on the earliest date for this dataset, 
                4 months back from the current date.

    Output: DataFrame containing regional solar generation data, also saved as CSV. Columns include 'Region', 'Timestamp', 
    'Solar_Generation_MW', and 'CapacityPercent'.
    '''
    
    # Filtering condition if date is given or none:
    if chosen_date is None:
        anchor_dt = pd.Timestamp.now()
    else:
        anchor_dt = pd.to_datetime(chosen_date)

    start_dt = anchor_dt - pd.Timedelta(days=window_days)  # 130 days back from anchor date
    end_dt = anchor_dt + pd.Timedelta(days=1)  # +1 day so includes full anchor date

    start_str = start_dt.strftime('%Y-%m-%d')
    end_str = end_dt.strftime('%Y-%m-%d')
    date_label = anchor_dt.strftime("%Y-%m-%d")

    where_clause = f"date_heure >= '{start_str}' AND date_heure < '{end_str}'"

    params = {
        "where": where_clause,
        "select": "libelle_region, date_heure, solaire, tch_solaire",
        "limit": -1}

    print('Downloading real-time data range: {} to {}...'.format(start_str, end_str))
    response = requests.get(tr_url, params=params)
    
    if response.status_code != 200 or not response.json():
        print(f"Failed to fetch data: HTTP {response.status_code}")
        return pd.DataFrame()

    # Re-naming Frence columns names:
    solar_PV_df_tr = (
        pd.DataFrame(response.json())
        .rename(columns={
            'libelle_region': 'Region',
            'date_heure': 'Timestamp',
            'solaire': 'Solar_Generation_MW',
            'tch_solaire': 'CapacityPercent'
        })
        .assign(Timestamp=lambda x: pd.to_datetime(x['Timestamp']))
        .sort_values(by=['Region', 'Timestamp'])
        .reset_index(drop=True)
    )

    # Save output using chosen timestamp label in filename:
    os.makedirs(file_path, exist_ok=True)
    out_file = os.path.join(file_path, f"france_regional_solar_tr_from_{date_label}.csv")
    
    solar_PV_df_tr.to_csv(out_file, index=False)
    return solar_PV_df_tr



# Second source of data: consolidated/verified solar generation data (cons-def) from RTE's ODRÉ portal.
cons_url = "https://odre.opendatasoft.com/api/v2/catalog/datasets/eco2mix-regional-cons-def/exports/json"

def get_rte_solar_data_cons_def(cons_def_url, file_path, start_date="2020-01-01", end_date=None):
    '''
    Retrieves consolidated/verified solar generation data for France from RTE's ODRÉ portal, which starts 4 months before current 
    date and goes back to 2012.
    
    Inputs:
        cons_def_url (str): URL for the consolidated/verified dataset
        file_path (str): Folder directory where the output CSV will be saved.
        start_date (str): Start date string (format 'YYYY-MM-DD'). Defaults to '2020-01-01'.
        end_date (str, optional): End date string (format 'YYYY-MM-DD'). 
                                  If None, defaults to the current date.

    Output:
        DataFrame containing regional solar generation data, also saved as CSV. Columns include 'Region', 'Timestamp', 'Solar_Generation_MW',
        and 'CapacityPercent'.
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

    params = {
        "where": where_clause,
        "select": "libelle_region, date_heure, solaire, tch_solaire",
        "limit": -1}

    print(f"Downloading consolidated data range: {start_str} to {label_end_str}...")
    response = requests.get(cons_def_url, params=params)

    if response.status_code != 200:
        print(f"Extraction failed: HTTP {response.status_code}")
        return pd.DataFrame()

    records = response.json()
    if not records:
        print(f"No records found for range {start_str} to {label_end_str}.")
        return pd.DataFrame()

    # Renaming the French column names:
    df_cons = (
        pd.DataFrame(records)
        .rename(columns={
            'libelle_region': 'Region',
            'date_heure': 'Timestamp',
            'solaire': 'Solar_Generation_MW',
            'tch_solaire': 'CapacityPercent'
        })
        .assign(Timestamp=lambda x: pd.to_datetime(x['Timestamp']))
        .sort_values(by=["Region", "Timestamp"])
        .reset_index(drop=True)
    )

    os.makedirs(file_path, exist_ok=True)
    out_file = os.path.join(file_path, f"france_regional_solar_cons_def_{start_str}_to_{label_end_str}.csv")
    df_cons.to_csv(out_file, index=False)

    return df_cons


# Function used to pull from both sources and merge into a single master DataFrame:
def get_rte_solar_data_full(file_path, start_date="2020-01-01", end_date=None, tr_window_days=130):
    '''
    Wrapper to retrieve full historical and real-time solar generation data for France by combining 
    the consolidated (cons-def) and real-time (tr) RTE datasets.
    
    Inputs:
        file_path (str): Directory where the output CSV files will be saved.
        start_date (str): Historical start date (format 'YYYY-MM-DD'). Defaults to '2020-01-01'.
        end_date (str, optional): Target end date. Defaults to current date if None.
        tr_window_days (int): Lookback window in days for the real-time dataset (default 130).

    Output:
        DataFrame: Merged, deduplicated master DataFrame sorted by Region and Timestamp.
    '''
    
    anchor_dt = pd.Timestamp.now() if end_date is None else pd.to_datetime(end_date)
    anchor_str = anchor_dt.strftime('%Y-%m-%d')
    
    print("\n Downloading consolidated data (cons-def)...")
    df_cons = get_rte_solar_data_cons_def(file_path, start_date=start_date, end_date=anchor_dt)
    
    print("\n Downloading real-time data (tr)...")
    df_tr = get_rte_solar_data_tr(file_path, chosen_date=anchor_str, window_days=tr_window_days)

    df_list = [df for df in [df_cons, df_tr] if not df.empty]
    
    if not df_list:
        print("No data retrieved from either source.")
        return pd.DataFrame()

    df_full = (
        pd.concat(df_list, ignore_index=True)
        # Keep real-time ('tr') row if there happens to be an overlapping timestamp
        .drop_duplicates(subset=['Region', 'Timestamp'], keep='last')
        .sort_values(by=['Region', 'Timestamp'])
        .reset_index(drop=True)
    )

    start_label = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    out_file = os.path.join(file_path, f"france_regional_solar_master_{start_label}_to_{anchor_str}.csv")
    df_full.to_csv(out_file, index=False)

    return df_full

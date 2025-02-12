import os
import requests
from datetime import datetime, timedelta
import json
import pandas as pd
from tqdm import tqdm
import time
import argparse
import dotenv
import logging

# Configure logging
logging.basicConfig(
    filename="fetch_api.log", filemode="a",
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

#user passing parameters to the API as per reference https://ned.nl/nl/handleiding-api
parser = argparse.ArgumentParser(description ='API fetch for NL from ned.nl/nl')
parser.add_argument('--point', type = int, default=0, help ='What geographic area should the data cover? For e.g. point: 0 = Netherland, also contains other points and offshore data.')
parser.add_argument('--type', type = int, default=2, help ='What is the type of energy carrier? For e.g. type: 0 = all, 1 = wind, 2 = solar ...')
parser.add_argument('--granularity', type = int, default=4, help ='How data should be grouped in time interval For e.g. granularity: 1 = 1min, 2 = 5min, 3 = 10min, 4 = 15min, 5 = 1hour, 6 = day, 7 = month, 8 = year')
parser.add_argument('--granularitytimezone', type = int, default=0, help ='Name of timezone For e.g. granularitytimezone: 0 = UTC, 1 = Europe/Amsterdam')
parser.add_argument('--classification', type = int, default=2, help ='near realtime or prediction For e.g. classification: 1 = forecast, 2 = Current, 3 = Backcast')
parser.add_argument('--activity', type = int, default=1, help ='providing or consuming. For e.g. activity: 1 = providing, 2 = consuming, 3 = Import, 4 = Export, 5 = Storage in, 6 = Storage out, 7 = Storage')
parser.add_argument('--start_date', type=lambda d: datetime.strptime(d, "%Y-%m-%d"), default='2017-01-01', help ='The Start Date - format YYYY-MM-DD')
parser.add_argument('--end_date', type=lambda d: datetime.strptime(d, "%Y-%m-%d"), default='2025-01-01', help ='The Start Date - format YYYY-MM-DD')
args = parser.parse_args()

# load .env variables
dotenv.load_dotenv()

# API base URL
BASE_URL = "https://api.ned.nl/v1"

# Get API credentials from environment variables
API_KEY = os.getenv("APIKEY_NEDNL")

# Create session with default headers
session = requests.Session()
session.headers.update({
    "X-AUTH-TOKEN": API_KEY,
    "Accept": "application/ld+json"
})


def fetch_with_retry(session, url, params, max_retries=3, initial_delay=5):  # increased initial delay to 5 seconds
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, allow_redirects=False)
            
            if response.status_code == 200:
                # Add consistent delay after successful request
                time.sleep(0.1)  # ~180 requests per 5 minutes
                return response.json()
            
            if response.status_code == 429:
                wait_time = initial_delay * (2 ** attempt)  # exponential backoff
                logger.warning(f"Rate limit hit. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
                
            logger.error(f"Error: Status code {response.status_code}")
            logger.info("Response:", response.json())
            return None
            
        except Exception as e:
            logger.error(f"Request failed: {str(e)}")
            return None
    
    logger.warning("Max retries reached")
    return None

def nl_data(csv_dir: str = "../data"):
    """
    Save fetched API data to a CSV file

    Parameters:
        csv_dir (str, optional): Directory to save CSV files
    """
    

    # Initialize empty DataFrame to store all results
    all_data = pd.DataFrame()

    # Define date range
    start_date = datetime(args.start_date.year, args.start_date.month, args.start_date.day)
    end_date = datetime(args.end_date.year, args.end_date.month, args.end_date.day)
    current_date = start_date

    if not csv_dir: #check if directory csv directory provided
        raise ValueError("CSV directory is not provided for CSV saving.")
    
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, f"nednl_15m_solar_startdate_{start_date}_enddate_{end_date}.csv")

    # Calculate total number of days for progress bar
    total_days = (end_date - start_date).days

    # Create progress bar
    for _ in tqdm(range(total_days), desc="Processing dates"):
        # Calculate next day
        next_date = current_date + timedelta(days=1)
        
        # Use existing session and BASE_URL from above
        url = f"{BASE_URL}/utilizations"
        
        params = {
            'point': args.point,
            'type': args.type,
            'granularity': args.granularity, 
            'granularitytimezone': args.granularitytimezone,
            'classification': args.classification,
            'activity': args.activity,
            'validfrom[strictly_before]': next_date.strftime('%Y-%m-%d'),
            'validfrom[after]': current_date.strftime('%Y-%m-%d')
        }

        data = fetch_with_retry(session, f"{BASE_URL}/utilizations", params)


        # Extract utilization data into a DataFrame
        utilizations = data['hydra:member']
        
        if utilizations:
            # Create DataFrame for current day
            df = pd.DataFrame([{
                'id': util['id'],
                'point': util['point'],
                'type': util['type'],
                'granularity': util['granularity'], 
                'activity': util['activity'],
                'classification': util['classification'],
                'capacity (kW)': util['capacity'],
                'volume (kWh)': util['volume'],
                'percentage': util['percentage'],
                # 'emission': util['emission'],
                # 'emissionfactor': util['emissionfactor'],
                'validfrom (UTC)': datetime.fromisoformat(util['validfrom']),
                'validto (UTC)': datetime.fromisoformat(util['validto']),
                'lastupdate (UTC)': datetime.fromisoformat(util['lastupdate'])
            } for util in utilizations])
            
            # Append to main DataFrame
            all_data = pd.concat([all_data, df], ignore_index=True)
        
        current_date = next_date

    all_data['validfrom (UTC)'] = pd.to_datetime(all_data['validfrom (UTC)'])

    # Sort final DataFrame by timestamp
    all_data = all_data.sort_values('validfrom (UTC)')

    # Drop unnecessary columns
    all_data = all_data.drop(columns=['id', 'point', 'type', 'granularity', 'activity', 'classification','percentage'])#, 'emission', 'emissionfactor'])

    logger.info(f"Final DataFrame shape: {all_data.shape}")
    all_data.head()
    logger.info(f"Saved the data fetched from API at {csv_path}")
    #all_data.to_csv(csv_path, index=False)
    return all_data

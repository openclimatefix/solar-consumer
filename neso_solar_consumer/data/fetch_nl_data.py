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

def nl_data():
    """
    Save fetched API data to a CSV file

    Parameters:
        csv_dir (str, optional): Directory to save CSV files
    """
    

    # Initialize empty DataFrame to store all results
    all_data = pd.DataFrame()

    # Define date range
    
    end_date = datetime.now() 
    start_date = end_date - timedelta(days=2)
    current_date = start_date
    
    # Calculate total number of days for progress bar
    total_days = (end_date - start_date).days

    # Create progress bar
    for _ in tqdm(range(total_days), desc="Processing dates"):
        # Calculate next day
        next_date = current_date + timedelta(days=2)
        
        # Use existing session and BASE_URL from above
        url = f"{BASE_URL}/utilizations"
        
        params = {
            'point': 0,
            'type': 2,
            'granularity': 4, 
            'granularitytimezone': 0,
            'classification': 2,
            'activity': 1,
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

    return all_data

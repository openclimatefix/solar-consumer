"""
Script to fetch NESO Solar Forecast Data
This script provides functions to fetch solar forecast data from the NESO API.
The data includes solar generation estimates for embedded solar farms and combines
date and time fields into a single timestamp for further analysis.
"""

import urllib.request
import urllib.parse
import json
import pandas as pd
from neso_solar_consumer.data.fetch_gp_data import gb_data


def fetch_data(country: str = "gb") -> pd.DataFrame:

    if country == "gb":
        try:
            df = gb_data()

        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()

    else:
        error = "Only UK and Netherlands data can be fetched at the moment"
        print(error)

    return df


def fetch_data_using_sql(sql_query: str) -> pd.DataFrame:
    """
    Fetch data from the NESO API using an SQL query, process it, and return a DataFrame.

    Parameters:
        sql_query (str): The SQL query to fetch data from the API.

    Returns:
        pd.DataFrame: A DataFrame containing two columns:
                      - `Datetime_GMT`: Combined date and time in UTC.
                      - `solar_forecast_kw`: Estimated solar forecast in kW.
    """
    base_url = "https://api.neso.energy/api/3/action/datastore_search_sql"
    encoded_query = urllib.parse.quote(sql_query)
    url = f"{base_url}?sql={encoded_query}"

    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode("utf-8"))
        records = data["result"]["records"]

        # Create DataFrame from records
        df = pd.DataFrame(records)

        # Parse and combine DATE_GMT and TIME_GMT into Datetime_GMT
        df["Datetime_GMT"] = pd.to_datetime(
            df["DATE_GMT"].str[:10] + " " + df["TIME_GMT"].str.strip(),
            format="%Y-%m-%d %H:%M",
            errors="coerce",
        ).dt.tz_localize("UTC")

        # Rename and select necessary columns
        df = df.rename(columns={"EMBEDDED_SOLAR_FORECAST": "solar_forecast_kw"})
        df = df[["Datetime_GMT", "solar_forecast_kw"]]

        # Drop rows with invalid Datetime_GMT
        df = df.dropna(subset=["Datetime_GMT"])

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

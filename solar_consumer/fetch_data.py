"""
Script to fetch NESO Solar Forecast Data
This script provides functions to fetch solar forecast data from the NESO API
or other country-specific sources like UPSLDC (India).
"""

import urllib.request
import urllib.parse
import json
import pandas as pd

from solar_consumer.data.fetch_gb_data import fetch_gb_data
from solar_consumer.data.fetch_nl_data import fetch_nl_data
from solar_consumer.data.fetch_in_data import fetch_in_data


def fetch_data(country: str = "gb", historic_or_forecast: str = "forecast") -> pd.DataFrame:
    """
    Fetch data based on the country and whether it's forecast or generation data.

    Args:
        country (str): Country code ('gb', 'nl', or 'in').
        historic_or_forecast (str): 'forecast' or 'generation'

    Returns:
        pd.DataFrame: Fetched data with standardized columns:
            - target_datetime_utc (datetime)
            - solar_generation_kw (float)
    """

    country = country.lower()

    if country in ("gb", "uk"):
        if historic_or_forecast != "forecast":
            raise ValueError("Only forecast data is supported for GB (UK).")
        return fetch_gb_data(historic_or_forecast=historic_or_forecast)

    elif country in ("nl", "netherlands"):
        return fetch_nl_data(historic_or_forecast=historic_or_forecast)

    elif country in ("in", "india"):
        if historic_or_forecast != "generation":
            raise ValueError("Only generation data is supported for India.")
        return fetch_in_data(historic_or_forecast=historic_or_forecast)

    else:
        raise ValueError(f"Unsupported country: {country}. Supported: 'gb', 'nl', 'in'.")


def fetch_data_using_sql(sql_query: str) -> pd.DataFrame:
    """
    Fetch data from the NESO API using an SQL query, process it, and return a DataFrame.

    Parameters:
        sql_query (str): The SQL query to fetch data from the API.

    Returns:
        pd.DataFrame: A DataFrame containing:
                      - target_datetime_utc (datetime)
                      - solar_generation_kw (float)
    """
    base_url = "https://api.neso.energy/api/3/action/datastore_search_sql"
    encoded_query = urllib.parse.quote(sql_query)
    url = f"{base_url}?sql={encoded_query}"

    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode("utf-8"))
        records = data["result"]["records"]

        df = pd.DataFrame(records)

        df["Datetime_GMT"] = pd.to_datetime(
            df["DATE_GMT"].str[:10] + " " + df["TIME_GMT"].str.strip(),
            format="%Y-%m-%d %H:%M",
            errors="coerce",
        ).dt.tz_localize("UTC")

        df = df.rename(columns={"EMBEDDED_SOLAR_FORECAST": "solar_forecast_kw"})
        df = df[["Datetime_GMT", "solar_forecast_kw"]]
        df = df.dropna(subset=["Datetime_GMT"])

        df.rename(
            columns={
                "solar_forecast_kw": "solar_generation_kw",
                "Datetime_GMT": "target_datetime_utc",
            },
            inplace=True,
        )

        return df

    except Exception as e:
        print(f"An error occurred while executing SQL query: {e}")
        return pd.DataFrame()

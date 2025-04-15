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
from solar_consumer.data.fetch_gb_data import fetch_gb_data
from solar_consumer.data.fetch_nl_data import fetch_nl_data


def fetch_data(country: str = "gb", historic_or_forecast: str = "forecast") -> pd.DataFrame:
    """
    Get data from different countries

    :param country: "gb", or "nl"
    :param historic_or_forecast: "generation" or "forecast"
    :return: Pandas dataframe with the following columns:
        target_datetime_utc: Combined date and time in UTC.
        solar_generation_kw: Solar generation in kW. Can be a forecast, or historic values
    """

    country_data_functions = {"gb": fetch_gb_data, "nl": fetch_nl_data}

    if country in country_data_functions:
        try:
            data = country_data_functions[country](historic_or_forecast=historic_or_forecast)

            assert "target_datetime_utc" in data.columns
            assert "solar_generation_kw" in data.columns

            return data

        except Exception as e:
            raise Exception(f"An error occurred while fetching data for {country}: {e}") from e

    else:
        print("Only UK and Netherlands data can be fetched at the moment")

    return pd.DataFrame()  # Always return a DataFrame (never None)


def fetch_data_using_sql(sql_query: str) -> pd.DataFrame:
    """
    Fetch data from the NESO API using an SQL query, process it, and return a DataFrame.

    Parameters:
        sql_query (str): The SQL query to fetch data from the API.

    Returns:
        pd.DataFrame: A DataFrame containing two columns:
                      - `target_datetime_utc`: Combined date and time in UTC.
                      - `solar_generation_kw`: Estimated solar forecast in kW.
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

        # rename columns to match the schema
        df.rename(
            columns={
                "solar_forecast_kw": "solar_generation_kw",
                "Datetime_GMT": "target_datetime_utc",
            },
            inplace=True,
        )

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

"""
Test Suite for `fetch_data` and `fetch_data_using_sql` Functions

This script validates the functionality and consistency of two data-fetching functions:
- `fetch_data`: Fetches data via API (mocked).
- `fetch_data_using_sql`: Fetches data via SQL query (mocked).

### How to Run the Tests:

Run the entire test suite:
    pytest tests/test_fetch_data.py

Run with verbose output:
    pytest tests/test_fetch_data.py -v

Run tests matching a specific pattern:
    pytest tests/test_fetch_data.py -k "fetch_data"
"""
import pytest
import os

from solar_consumer.fetch_data import fetch_data, fetch_data_using_sql
from unittest.mock import patch, Mock
import json
import pandas as pd
from solar_consumer.data.fetch_nl_data import fetch_nl_data

# TODO update
#
# def test_fetch_data_mock_success(test_config):
#     """
#     Test `fetch_data` with a mocked successful API response using `test_config`.
#     """
#     mock_response = {
#         "result": {
#             "records": [
#                 {
#                     "DATE_GMT": "2025-01-14",
#                     "TIME_GMT": "05:30",
#                     "EMBEDDED_SOLAR_FORECAST": 0,
#                 },
#                 {
#                     "DATE_GMT": "2025-01-14",
#                     "TIME_GMT": "06:00",
#                     "EMBEDDED_SOLAR_FORECAST": 101,
#                 },
#                 {
#                     "DATE_GMT": "2025-01-14",
#                     "TIME_GMT": "06:30",
#                     "EMBEDDED_SOLAR_FORECAST": 200,
#                 },
#                 {
#                     "DATE_GMT": "2025-01-14",
#                     "TIME_GMT": "07:00",
#                     "EMBEDDED_SOLAR_FORECAST": 300,
#                 },
#                 {
#                     "DATE_GMT": "2025-01-14",
#                     "TIME_GMT": "07:30",
#                     "EMBEDDED_SOLAR_FORECAST": 400,
#                 },
#             ]
#         }
#     }
#
#     # Mock API response as bytes
#     with patch("neso_solar_consumer.fetch_data.urllib.request.urlopen") as mock_urlopen:
#         mock_urlopen.return_value.read.return_value = json.dumps(mock_response).encode(
#             "utf-8"
#         )
#         df = fetch_data()
#
#         # Assertions
#         assert not df.empty, "Expected non-empty DataFrame for successful API response!"
#         assert list(df.columns) == [
#             "Datetime_GMT",
#             "solar_forecast_kw",
#         ], "Unexpected DataFrame columns!"
#         assert (
#             len(df) == test_config["limit"]
#         ), f"Expected DataFrame to have {test_config['limit']} rows!"
#         print("Mocked DataFrame from fetch_data (success):")
#         print(df)


def test_fetch_data_mock_failure(test_config):
    """
    Test `fetch_data` with a mocked API failure using `test_config`.
    """
    with patch("solar_consumer.fetch_data.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = Exception("API failure simulated")

        with pytest.raises(Exception):
            _ = fetch_data()



def test_fetch_data_using_sql_mock_success(test_config):
    """
    Test `fetch_data_using_sql` with a mocked successful SQL query result using `test_config`.
    """
    mock_response = {
        "result": {
            "records": [
                {
                    "DATE_GMT": "2025-01-14",
                    "TIME_GMT": "05:30",
                    "EMBEDDED_SOLAR_FORECAST": 0,
                },
                {
                    "DATE_GMT": "2025-01-14",
                    "TIME_GMT": "06:00",
                    "EMBEDDED_SOLAR_FORECAST": 101,
                },
                {
                    "DATE_GMT": "2025-01-14",
                    "TIME_GMT": "06:30",
                    "EMBEDDED_SOLAR_FORECAST": 200,
                },
                {
                    "DATE_GMT": "2025-01-14",
                    "TIME_GMT": "07:00",
                    "EMBEDDED_SOLAR_FORECAST": 300,
                },
                {
                    "DATE_GMT": "2025-01-14",
                    "TIME_GMT": "07:30",
                    "EMBEDDED_SOLAR_FORECAST": 400,
                },
            ]
        }
    }

    # Mock API response as bytes
    with patch("solar_consumer.fetch_data.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.read.return_value = json.dumps(mock_response).encode(
            "utf-8"
        )
        sql_query = (
            f'SELECT * FROM "{test_config["resource_id"]}" LIMIT {test_config["limit"]}'
        )
        df = fetch_data_using_sql(sql_query)

        # Assertions
        assert not df.empty, "Expected non-empty DataFrame for successful SQL query!"
        assert list(df.columns) == [
            "target_datetime_utc",
            "solar_generation_kw",
        ], "Unexpected DataFrame columns!"
        assert (
            len(df) == test_config["limit"]
        ), f"Expected DataFrame to have {test_config['limit']} rows!"
        print("Mocked DataFrame from fetch_data_using_sql (success):")
        print(df)


def test_fetch_data_using_sql_mock_failure(test_config):
    """
    Test `fetch_data_using_sql` with a mocked failure using `test_config`.
    """
    with patch("solar_consumer.fetch_data.urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = Exception("SQL query failure simulated")
        sql_query = (
            f'SELECT * FROM "{test_config["resource_id"]}" LIMIT {test_config["limit"]}'
        )
        df = fetch_data_using_sql(sql_query)

        # Assertions
        assert df.empty, "Expected an empty DataFrame when SQL query fails!"
        print("Mocked DataFrame from fetch_data_using_sql (failure):")
        print(df)


@patch("solar_consumer.data.fetch_nl_data.requests.Session.get")
def test_fetch_nl_data(mock_api, nl_mock_data):

    # Configure the mock to return a response with the mock data
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = nl_mock_data
    mock_api.return_value = mock_response

    df = fetch_nl_data(historic_or_forecast='historic')

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "capacity (kW)" in df.columns
    assert "volume (kWh)" in df.columns

def test_gb_historic_inday():

    # set enviormental variable REGIME to inday
    os.environ["UK_PVLIVE_REGIME"] = "in-day"

    df = fetch_data(country = "gb", historic_or_forecast = "historic")

    # 10 GSPs for 2 hours is 
    assert 30<=len(df) <=40
    # If run at start of 30 mins, its 30, but if run after new results come out its 40


def test_gb_historic_day_after():

    # set enviormental variable REGIME to inday
    os.environ["UK_PVLIVE_REGIME"] = "day-after"

    df = fetch_data(country = "gb", historic_or_forecast = "historic")

    # 10 GSPs for 24 hours is at 30 minutes periods, including the extra two at end
    assert len(df) == 10*50

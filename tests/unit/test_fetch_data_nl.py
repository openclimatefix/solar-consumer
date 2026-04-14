"""
Test Suite for `fetch_data` for the NL
"""

from unittest.mock import patch, Mock
import numpy as np
import pandas as pd
from solar_consumer.data.fetch_nl_data import fetch_nl_data, check_national_capacity_equals_regional_sum


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

@patch("solar_consumer.data.fetch_nl_data.requests.Session.get")
def test_fetch_nl_data_small_percentage(mock_api, nl_mock_data_small_percentage):

    # Configure the mock to return a response with the mock data
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = nl_mock_data_small_percentage
    mock_api.return_value = mock_response

    df = fetch_nl_data(historic_or_forecast='historic')

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "capacity (kW)" in df.columns
    assert "volume (kWh)" in df.columns
    assert df["capacity_kw"].isna().all()

def test_check_national_capacity_equals_regional_sum_not_all_regions():

    # set up the data, so that not all regions are there, therefore capacity_kw should be nan
    data = pd.DataFrame({
        "region_id": [0, 0, 1, 1, 2, 2],
        "capacity_kw": [300, 300, 100, 150, 200, 250],
        "target_datetime_utc": pd.to_datetime(["2025-01-14 05:30:00", "2025-01-14 06:00:00"]*3)
    })

    result = check_national_capacity_equals_regional_sum(data)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    # check no nans in capacity_kw
    assert result["capacity_kw"].isna().all()

def test_check_national_capacity_equals_regional_sum():

    # set up the data, so 
    # 1. first data point the capacities do add up
    # 2. second data point the capacities do not add up
    # 3. has a NaN in it, so all capacity_kw should be NaN
    data = []
    for i in range(13):
        data.append({
            "region_id": i,
            "capacity_kw": i*10 if i>0 else 10*sum(range(13)), # 780
            "target_datetime_utc": pd.to_datetime("2025-01-14 05:30:00")
        })
        # regionals won't add up to the national
        data.append({
            "region_id": i,
            "capacity_kw": 10*i,
            "target_datetime_utc": pd.to_datetime("2025-01-14 06:00:00")
        })
        # this time stamp has a NaN in it, so don't check this one
        data.append({
            "region_id": i,
            "capacity_kw": 20*(i+1) if i != 5 else np.nan,
            "target_datetime_utc": pd.to_datetime("2025-01-14 06:30:00")
        })
    data = pd.DataFrame(data)

    result = check_national_capacity_equals_regional_sum(data)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == 39 # 3 timestamps * 13 regions
    assert result["capacity_kw"].iloc[0] == 780
    assert not result["update_capacity"].iloc[1]
    assert not result["update_capacity"].iloc[2]
    assert result["capacity_kw"].iloc[3] == 10
    assert not result["update_capacity"].iloc[4]
    assert result["capacity_kw"].iloc[6] == 20
    assert not result["update_capacity"].iloc[7]
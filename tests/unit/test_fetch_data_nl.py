"""
Test Suite for `fetch_data` for the NL
"""

from unittest.mock import patch, Mock
import numpy as np
import pandas as pd
from solar_consumer.data.fetch_nl_data import fetch_nl_data, check_national_capacity_equals_regional_sum
from solar_consumer.data.fetch_nl_data import get_entsoe_day_prices, make_potential_generation
from unittest.mock import MagicMock


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
    assert not df["update_capacity"].all()

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
    # as there are not all the regions, update_capacity should be False
    assert not result["update_capacity"].all()

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
            "target_datetime_utc": pd.to_datetime("2025-01-14 05:30:00"),
            "update_capacity": True
        })
        # regionals won't add up to the national
        data.append({
            "region_id": i,
            "capacity_kw": 10*i,
            "target_datetime_utc": pd.to_datetime("2025-01-14 06:00:00"),
            "update_capacity": True
        })
        # this time stamp has a NaN in it, so don't check this one
        # note capacity_kw is NaN for region 5
        # note the regional capacities do add up to the national
        data.append({
            "region_id": i,
            "capacity_kw": [73,1,2,3,4,np.nan,6,7,8,9,10,11,12][i],
            "target_datetime_utc": pd.to_datetime("2025-01-14 06:30:00"),
            "update_capacity": True
        })
    data = pd.DataFrame(data)

    result = check_national_capacity_equals_regional_sum(data)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert len(result) == 39 # 3 timestamps * 13 regions
    assert result["update_capacity"].loc[::3].all() # all capacities can be updated
    assert not result["update_capacity"].loc[1::3].any() # no capacities can be updated
    assert not result["update_capacity"].loc[2::3].any() # no capacities can be update




@patch("solar_consumer.data.fetch_nl_data.EntsoePandasClient")
def test_get_entsoe_day_prices(mock_entsoe_pandas_client):

    start = pd.Timestamp('2026-05-12').tz_localize('UTC')
    end = pd.Timestamp('2026-05-13').tz_localize('UTC')
    api_key = ''

    mock_da_prices = pd.DataFrame({
        'target_datetime_utc': pd.date_range(start=start, end=end, freq='15min'),
        'price': [10] * 97
    })
    mock_da_prices.set_index('target_datetime_utc', inplace=True)
    mock_entsoe_pandas_client_instance = MagicMock()
    mock_entsoe_pandas_client.return_value = mock_entsoe_pandas_client_instance
    mock_entsoe_pandas_client_instance.query_day_ahead_prices.return_value = mock_da_prices

    prices = get_entsoe_day_prices(start=start, end=end, api_key=api_key)
    assert prices is not None
    assert isinstance(prices, pd.DataFrame)
    assert not prices.empty
    assert len(prices) == 97 # 15 minute intervals in one day + 1


@patch("solar_consumer.data.fetch_nl_data.EntsoePandasClient")
def test_make_potential_generation(mock_entsoe_pandas_client):
    start = pd.Timestamp('2026-05-10').tz_localize('UTC')
    end = pd.Timestamp('2026-05-11').tz_localize('UTC')

    data = pd.DataFrame({
        'target_datetime_utc': pd.date_range(start=start, end=end, freq='15min'),
        'solar_generation_kw': [1] * 97
    })

    mock_da_prices = pd.DataFrame({
        'target_datetime_utc': pd.date_range(start=start, end=end, freq='15min'),
        'price': [10] * 33 + [-1] * 25 + [1]* 39
    })
    mock_da_prices.set_index('target_datetime_utc', inplace=True)
    mock_entsoe_pandas_client_instance = MagicMock()
    mock_entsoe_pandas_client.return_value = mock_entsoe_pandas_client_instance
    mock_entsoe_pandas_client_instance.query_day_ahead_prices.return_value = mock_da_prices

    potential_generation = make_potential_generation(data=data)
    assert potential_generation is not None
    assert isinstance(potential_generation, pd.DataFrame)
    assert not potential_generation.empty
    assert len(potential_generation) == 97 # 15 minute intervals in one day + 1
    assert potential_generation.iloc[0].solar_generation_no_curtailment_kw == 1 # 00:00 is 1
    assert potential_generation.iloc[48].solar_generation_no_curtailment_kw == 1.11 #12:00 has been increased

    # we know that the prices at 8:15 to 14:15 was negative
    # lets check these values
    negative_prices_idx = (potential_generation['target_datetime_utc'].dt.time >= pd.Timestamp('08:15').time()) \
        & (data['target_datetime_utc'].dt.time <= pd.Timestamp('14:15').time())
    # check the negative prices are all zero
    assert (potential_generation.loc[negative_prices_idx, "solar_generation_no_curtailment_kw"]== 1.11).all()
    assert (potential_generation.loc[~negative_prices_idx, "solar_generation_no_curtailment_kw"]== 1).all()
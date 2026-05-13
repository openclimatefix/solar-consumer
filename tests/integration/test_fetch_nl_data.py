""" Function to check getting the nl data

The following envs need to be set
- APIKEY_ENTSOE
"""

from solar_consumer.data.fetch_nl_data import get_entsoe_day_prices, make_potential_generation
import pandas as pd
import os

def test_get_entsoe_day_prices():

    start = pd.Timestamp('2026-05-12').tz_localize('UTC')
    end = pd.Timestamp('2026-05-13').tz_localize('UTC')
    api_key = os.getenv("APIKEY_ENTSOE")

    prices = get_entsoe_day_prices(start=start, end=end, api_key=api_key)
    assert prices is not None
    assert isinstance(prices, pd.DataFrame)
    assert not prices.empty
    assert len(prices) == 97 # 15 minute intervals in one day + 1


def test_make_potential_generation():
    start = pd.Timestamp('2026-05-10').tz_localize('UTC')
    end = pd.Timestamp('2026-05-11').tz_localize('UTC')

    data = pd.DataFrame({
        'target_datetime_utc': pd.date_range(start=start, end=end, freq='15T'),
        'solar_generation_kw': [1] * 97
    })

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
import pandas as pd
from solar_consumer.data.fetch_in_data import fetch_in_data

def test_fetch_in_data_live():

    df = fetch_in_data(historic_or_forecast="generation")

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "target_datetime_utc" in df.columns
    assert "solar_generation_kw" in df.columns
    assert df["solar_generation_kw"].iloc[0] > 0
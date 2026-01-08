# integration test
import pytest
from datetime import datetime, timedelta, timezone
from solar_consumer.data.fetch_ind_rajasthan_data import fetch_ind_rajasthan_data

@pytest.mark.skip(reason="Live RUVNL API is unstable and only accessible in India")
@pytest.mark.integration
def test_ind_rajasthan_data():
    """
    Integration test for India solar forecast fetching.

    Ensures that the RUVNL API integration returns both:
    - national (India)
    - at least one regional forecast
    """

    df = fetch_ind_rajasthan_data()

    assert not df.empty, "India API returned no forecast data"

    assert set(
        ["target_datetime_utc", "solar_generation_kw", "energy_type"]
    ).issubset(df.columns)


    # Check timestamps are within expected range (last 1 day by default)
    now_utc = datetime.now(timezone.utc)
    one_day_ago = now_utc - timedelta(days=1)

    assert df["target_datetime_utc"].min() >= one_day_ago
    assert df["target_datetime_utc"].max() <= now_utc

    # Sanity check solar generation values (kW)
    assert (df["solar_generation_kw"] >= 0).all()
    assert (df["solar_generation_kw"] <= 20_000_000).all()

    # Check solar is in energy_type column
    assert "solar" in df['energy_type'].values
    assert "wind" in df['energy_type'].values
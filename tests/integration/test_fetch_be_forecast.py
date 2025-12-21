# integration test
import pytest
from datetime import datetime, timedelta, timezone
from solar_consumer.data.fetch_be_data import fetch_be_data_forecast

@pytest.mark.integration
def test_be_forecast_contains_national_and_regional():
    """
    Integration test for Belgium solar forecast fetching.

    Ensures that the Elia API integration returns both:
    - national (Belgium)
    - at least one regional forecast
    """

    df = fetch_be_data_forecast()

    assert not df.empty, "Elia API returned no forecast data"

    assert set(
        ["target_datetime_utc", "solar_generation_kw", "region", "forecast_type"]
    ).issubset(df.columns)

    regions = df["region"].unique()

    assert "Belgium" in regions, "National Belgium forecast missing"

    regional_regions = [r for r in regions if r != "Belgium"]
    assert regional_regions, "No regional Belgium forecasts found"

    # Check timestamps are within expected range (last 1 day by default)
    now_utc = datetime.now(timezone.utc)
    one_day_ago = now_utc - timedelta(days=1)

    assert df["target_datetime_utc"].min() >= one_day_ago
    assert df["target_datetime_utc"].max() <= now_utc

    # Sanity check solar generation values (kW)
    assert (df["solar_generation_kw"] >= 0).all()
    assert (df["solar_generation_kw"] <= 20_000_000).all()

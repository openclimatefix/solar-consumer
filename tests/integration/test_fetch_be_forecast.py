# integration test

import pytest
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

    if df.empty:
        pytest.skip("Elia API returned no data")

    assert set(
        ["target_datetime_utc", "solar_generation_kw", "region", "forecast_type"]
    ).issubset(df.columns)

    regions = df["region"].unique()

    assert "Belgium" in regions, "National Belgium forecast missing"

    regional_regions = [r for r in regions if r != "Belgium"]
    assert regional_regions, "No regional Belgium forecasts found"

"""
Integration Test for Fetching, Formatting, and Saving Forecast Data

This test validates the complete pipeline:
1. Fetch data from the NESO API.
2. Format the data into `ForecastSQL` objects.
3. Save the formatted forecasts to the database.
4. Verify that the forecasts were correctly saved.

### How to Run
Execute this test using `pytest`:
    pytest tests/test_real_forecasts.py

To view detailed logs during execution, use:
    pytest -s tests/test_real_forecasts.py
"""

import pytest
from nowcasting_datamodel.models import ForecastSQL
from neso_solar_consumer.fetch_data import fetch_data
from neso_solar_consumer.format_forecast import format_to_forecast_sql
from neso_solar_consumer.save_forecast import save_forecasts


@pytest.mark.integration
def test_real_forecasts(db_session, test_config):
    """
    Integration Test: Validates the full pipeline of fetching, formatting, and saving
    forecasts.
    """
    # Step 1: Fetch data from the NESO API
    df = fetch_data()

    assert not df.empty, "fetch_data returned an empty DataFrame!"
    assert set(df.columns) == {
        "Datetime_GMT",
        "solar_forecast_kw",
    }, "Unexpected DataFrame columns!"

    # Step 2: Format the fetched data into ForecastSQL objects
    forecasts = format_to_forecast_sql(
        data=df,
        model_tag=test_config["model_name"],
        model_version=test_config["model_version"],
        session=db_session,
    )
    assert forecasts, "No forecasts were generated from the fetched data!"
    forecast = forecasts[0]
    assert len(forecast.forecast_values) == len(df), (
        f"Mismatch in forecast values! Expected {len(df)}, "
        f"but got {len(forecast.forecast_values)}."
    )

    # Step 3: Save formatted forecasts to the database
    save_forecasts(forecasts=forecasts, session=db_session)

    # Step 4: Validate that the forecasts were saved correctly
    saved_forecast = db_session.query(ForecastSQL).first()
    assert saved_forecast is not None, "No forecast was saved to the database!"
    assert (
        saved_forecast.model.name == test_config["model_name"]
    ), "Model name mismatch!"
    assert len(saved_forecast.forecast_values) > 0, "No forecast values were saved!"

    # Additional assertions for saved data consistency
    saved_values = saved_forecast.forecast_values
    for original_row, saved_value in zip(df.itertuples(), saved_values):
        assert (
            saved_value.target_time == original_row.Datetime_GMT
        ), "Mismatch in target_time!"
        assert saved_value.expected_power_generation_megawatts == pytest.approx(
            original_row.solar_forecast_kw / 1000
        ), "Mismatch in expected power generation!"

    print("\nIntegration test completed successfully.")

from solar_consumer.fetch_data import fetch_data
from solar_consumer.format_forecast import format_to_forecast_sql
from unittest.mock import patch
import pandas as pd


def test_format_to_forecast_sql_real(db_session, test_config):
    """
    Test `format_to_forecast_sql` with mocked API data and real PostgreSQL interactions.

    Steps:
    1. Mock the `fetch_data` function to return controlled input data.
    2. Format the mocked data into a ForecastSQL object.
    3. Validate the content and consistency of the ForecastSQL object and PostgreSQL process.
    """
    # Mock input data consistent with fetch_data tests
    mock_data = pd.DataFrame(
        {
            "target_datetime_utc": pd.Series(
                pd.to_datetime(
                    [
                        "2025-01-14 05:30:00",
                        "2025-01-14 06:00:00",
                        "2025-01-14 06:30:00",
                        "2025-01-14 07:00:00",
                        "2025-01-14 07:30:00",
                    ]
                )
            ).dt.tz_localize(
                "UTC"
            ),  # Ensure UTC timezone matches `fetch_data`
            "solar_generation_kw": [0, 101, 200, 300, 400],
        }
    )

    # Patch `fetch_data` to return the mock data
    with patch("solar_consumer.fetch_data.fetch_data", return_value=mock_data):
        # Step 1: Fetch data (mocked)
        data = fetch_data()

        assert not data.empty, "fetch_data returned an empty DataFrame!"
        assert set(data.columns) == {
            "target_datetime_utc",
            "solar_generation_kw",
        }, "Unexpected DataFrame columns!"

        # Step 2: Format the data into a ForecastSQL object
        forecasts = format_to_forecast_sql(
            data, test_config["model_name"], test_config["model_version"], db_session
        )
        assert (
            len(forecasts) == 1
        ), f"Expected 1 ForecastSQL object, got {len(forecasts)}"

        # Step 3: Validate the ForecastSQL content
        forecast = forecasts[0]

        # Ensure the number of `ForecastValue` entries matches the number of rows in the input data
        assert len(forecast.forecast_values) == len(
            data
        ), f"Mismatch in ForecastValue entries! Expected {len(data)} but got {len(forecast.forecast_values)}."

        # Validate individual `ForecastValue` entries
        for fv, (_, row) in zip(forecast.forecast_values, data.iterrows()):
            assert (
                fv.target_time == row["target_datetime_utc"]
            ), f"Mismatch in target_time for row {row}"
            expected_power_mw = row["solar_generation_kw"] / 1000  # Convert kW to MW
            assert fv.expected_power_generation_megawatts == expected_power_mw, (
                f"Mismatch in expected_power_generation_megawatts for row {row}. "
                f"Expected {expected_power_mw}, got {fv.expected_power_generation_megawatts}."
            )
